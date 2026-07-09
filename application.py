

from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import json
import logging
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import os

application = Flask(__name__)
app = application

# Configuration
REFRESH_INTERVAL = 5  # seconds

# DynamoDB Configuration
DYNAMODB_TABLE = 'dynamodb-x24315851'
AWS_REGION = 'us-east-1'

# Initialize DynamoDB client
try:
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)
    print(f"Connected to DynamoDB table: {DYNAMODB_TABLE}")
except Exception as e:
    print(f"Failed to connect to DynamoDB: {e}")
    table = None

# SNS Configuration (optional)
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:252250993625:sns-x24315851'
try:
    sns_client = boto3.client('sns', region_name=AWS_REGION)
    print("SNS client initialized")
except Exception as e:
    print(f"Failed to initialize SNS: {e}")
    sns_client = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Critical thresholds
CRITICAL_THRESHOLDS = {
    "voltage": {
        "min": 300, 
        "max": 410, 
        "warning_min": 320, 
        "warning_max": 400,
        "unit": "V",
        "display_name": "Voltage"
    },
    "current": {
        "max": 120, 
        "warning_max": 100,
        "min": -200,
        "unit": "A",
        "display_name": "Current"
    },
    "energy_throughput": {
        "max": 150, 
        "warning_max": 100,
        "min": 0,
        "unit": "kWh",
        "display_name": "Energy Throughput"
    },
    "temperature": {
        "max": 65, 
        "warning_max": 55,
        "min": -20,
        "unit": "C",
        "display_name": "Temperature"
    },
    "vibration": {
        "max": 5.0, 
        "warning_max": 3.0,
        "min": 0,
        "unit": "g",
        "display_name": "Vibration"
    },
    "coolant_temperature": {
        "max": 95, 
        "warning_max": 80,
        "min": -20,
        "unit": "C",
        "display_name": "Coolant Temperature"
    },
    "range": {
        "min": 0, 
        "max": 500,
        "unit": "km",
        "display_name": "Range"
    }
}

def convert_decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: convert_decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal_to_float(item) for item in obj]
    return obj

def fetch_from_dynamodb(vehicle_id=None, limit=100):
    """Fetch data directly from DynamoDB"""
    if not table:
        logger.error("DynamoDB table not available")
        return [], "DynamoDB table not available"
    
    try:
        if vehicle_id:
            response = table.query(
                KeyConditionExpression=Key('vehicle_id').eq(vehicle_id),
                Limit=limit,
                ScanIndexForward=False
            )
        else:
            # Scan with limit
            response = table.scan(Limit=limit)
        
        items = response.get('Items', [])
        
        # Convert Decimal to float
        items = convert_decimal_to_float(items)
        
        # Sort by timestamp descending
        items.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        logger.info(f"Fetched {len(items)} records from DynamoDB")
        return items, None
        
    except Exception as e:
        error_msg = f"DynamoDB error: {str(e)}"
        logger.error(error_msg)
        return [], error_msg

def determine_system_status(data):
    """Determine overall system status based on sensor data"""
    if not data or len(data) == 0:
        return "unknown", "No data available", [], None
    
    latest = data[0]
    
    critical_sensors = []
    warning_sensors = []
    
    sensors_to_check = ['voltage', 'current', 'energy_throughput', 'temperature', 'vibration', 'coolant_temperature']
    
    for sensor in sensors_to_check:
        value = latest.get(sensor)
        if value is not None:
            thresholds = CRITICAL_THRESHOLDS.get(sensor, {})
            
            # Check critical
            if 'min' in thresholds and value < thresholds['min']:
                critical_sensors.append(sensor)
            elif 'max' in thresholds and value > thresholds['max']:
                critical_sensors.append(sensor)
            # Check warning
            elif 'warning_min' in thresholds and value < thresholds['warning_min']:
                warning_sensors.append(sensor)
            elif 'warning_max' in thresholds and value > thresholds['warning_max']:
                warning_sensors.append(sensor)
    
    if len(critical_sensors) >= 2:
        status = "emergency"
        status_message = "EMERGENCY - Multiple critical parameters detected"
        issues = [f"{s}: {latest.get(s)}" for s in critical_sensors]
    elif len(critical_sensors) >= 1:
        status = "critical"
        status_message = f"CRITICAL - {len(critical_sensors)} parameter(s) at critical level"
        issues = [f"{s}: {latest.get(s)}" for s in critical_sensors]
    elif len(warning_sensors) >= 2:
        status = "warning"
        status_message = f"WARNING - {len(warning_sensors)} parameter(s) approaching critical levels"
        issues = [f"{s}: {latest.get(s)}" for s in warning_sensors]
    elif len(warning_sensors) >= 1:
        status = "caution"
        status_message = f"CAUTION - {len(warning_sensors)} parameter(s) at warning level"
        issues = [f"{s}: {latest.get(s)}" for s in warning_sensors]
    else:
        status = "normal"
        status_message = "NORMAL - All battery parameters within safe range"
        issues = []
    
    return status, status_message, issues, latest

@app.route("/")
def index():
    return render_template("dashboard.html", refresh_interval=REFRESH_INTERVAL)

@app.route("/api/data")
def get_data():
    """Get data from DynamoDB"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    vehicle_id = request.args.get('vehicle_id')
    limit = request.args.get('limit', 100, type=int)
    
    # Fetch from DynamoDB
    data, error = fetch_from_dynamodb(vehicle_id, limit)
    
    if error:
        return jsonify({"success": False, "error": error, "data": []}), 200
    
    if not data:
        return jsonify({"success": True, "data": [], "total_records": 0})
    
    # Filter by date if provided
    filtered = data
    if start_date:
        try:
            filtered = [d for d in filtered if d.get('timestamp', '') >= start_date]
        except:
            pass
    
    if end_date:
        try:
            filtered = [d for d in filtered if d.get('timestamp', '') <= end_date]
        except:
            pass
    
    # Add status to each record
    for record in filtered:
        status, status_message, _, _ = determine_system_status([record])
        record['status'] = status
        record['status_message'] = status_message
    
    # Determine overall status
    overall_status, overall_message, _, latest_data = determine_system_status(data)
    
    return jsonify({
        "success": True,
        "data": filtered,
        "total_records": len(filtered),
        "overall_status": overall_status,
        "overall_status_message": overall_message,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/vehicles")
def get_vehicles():
    """Get list of unique vehicles"""
    data, error = fetch_from_dynamodb(limit=1000)
    
    if error or not data:
        return jsonify({"vehicles": []})
    
    vehicles = list(set([d.get('vehicle_id', 'Unknown') for d in data if d.get('vehicle_id')]))
    return jsonify({"vehicles": vehicles})

@app.route("/api/stats")
def get_stats():
    """Get statistical summary"""
    data, error = fetch_from_dynamodb(limit=1000)
    
    if error or not data:
        return jsonify({"success": False, "stats": {}})
    
    def get_avg(key):
        values = [d.get(key, 0) for d in data if d.get(key) is not None]
        return round(sum(values) / len(values), 2) if values else 0
    
    stats = {
        "voltage": {
            "current": data[0].get('voltage', 0) if data else 0,
            "avg": get_avg('voltage'),
            "unit": "V"
        },
        "current": {
            "current": data[0].get('current', 0) if data else 0,
            "avg": get_avg('current'),
            "unit": "A"
        },
        "energy_throughput": {
            "current": data[0].get('energy_throughput', 0) if data else 0,
            "avg": get_avg('energy_throughput'),
            "unit": "kWh"
        },
        "temperature": {
            "current": data[0].get('temperature', 0) if data else 0,
            "avg": get_avg('temperature'),
            "unit": "C"
        },
        "vibration": {
            "current": data[0].get('vibration', 0) if data else 0,
            "avg": get_avg('vibration'),
            "unit": "g"
        },
        "coolant_temperature": {
            "current": data[0].get('coolant_temperature', 0) if data else 0,
            "avg": get_avg('coolant_temperature'),
            "unit": "C"
        },
        "range": {
            "current": data[0].get('range', 0) if data else 0,
            "avg": get_avg('range'),
            "unit": "km"
        },
        "total_records": len(data)
    }
    
    overall_status, overall_message, _, _ = determine_system_status(data)
    stats["overall_status"] = overall_status
    stats["overall_status_message"] = overall_message
    
    return jsonify({"success": True, "stats": stats})

@app.route("/api/status")
def get_status():
    """Get API connection status"""
    data, error = fetch_from_dynamodb(limit=1)
    
    return jsonify({
        "success": error is None,
        "error": error,
        "data_available": len(data) > 0 if data else False,
        "record_count": len(data) if data else 0,
        "dynamodb_connected": table is not None
    })

@app.route("/api/thresholds")
def get_thresholds():
    """Get threshold configuration"""
    return jsonify(CRITICAL_THRESHOLDS)

if __name__ == "__main__":
    print("=" * 60)
    print("EV Battery Monitoring Dashboard")
    print("=" * 60)
    print(f"DynamoDB Table: {DYNAMODB_TABLE}")
    print(f"Refresh Interval: {REFRESH_INTERVAL} seconds")
    print(f"DynamoDB Connected: {table is not None}")
    print("=" * 60)
    print("Dashboard URL: http://localhost:5000")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)