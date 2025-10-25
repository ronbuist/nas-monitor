#!/usr/bin/env python3
"""
WD MyCloud Ex2 Ultra NAS Monitor with MQTT Publishing and Fan Control
Monitors NAS status and allows fan speed control via MQTT
"""

import json
import logging
import subprocess
import sys
import time
import signal
import requests
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Any

import yaml
import paho.mqtt.client as mqtt


class NASMonitor:
    """Main NAS monitoring class with MQTT publishing and fan control."""

    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the NAS monitor with configuration."""
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.mqtt_client = None
        self.running = False
        self.current_fan_mode = "unknown"  # Track current fan mode

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        if not config_file.exists():
            print(f"ERROR: Configuration file '{config_path}' not found!")
            print("Please create a config.yaml file. See config.yaml.example for reference.")
            sys.exit(1)

        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except yaml.YAMLError as e:
            print(f"ERROR: Invalid YAML in configuration file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Could not read configuration file: {e}")
            sys.exit(1)

    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        log_level = getattr(logging, self.config.get('logging', {}).get('level', 'INFO').upper())
        log_format = self.config.get('logging', {}).get('format',
                                   '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        logging.basicConfig(level=log_level, format=log_format)
        logger = logging.getLogger('nas_monitor')

        # Add file handler if specified
        log_file = self.config.get('logging', {}).get('file')
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            logger.addHandler(file_handler)

        return logger

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        if self.mqtt_client:
            self._publish_availability("offline")
            self.mqtt_client.disconnect()

    def _setup_mqtt(self) -> mqtt.Client:
        """Setup and configure MQTT client."""
        mqtt_config = self.config['mqtt']

        client = mqtt.Client(
            client_id=mqtt_config.get('client_id', 'nas_monitor'),
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )

        # Setup authentication if provided
        if 'username' in mqtt_config and 'password' in mqtt_config:
            client.username_pw_set(mqtt_config['username'], mqtt_config['password'])

        # Setup TLS if specified
        if mqtt_config.get('tls', {}).get('enabled', False):
            import ssl
            tls_config = mqtt_config['tls']
            client.tls_set(
                ca_certs=tls_config.get('ca_certs'),
                certfile=tls_config.get('certfile'),
                keyfile=tls_config.get('keyfile'),
                cert_reqs=ssl.CERT_REQUIRED if tls_config.get('cert_reqs', 'required') == 'required' else ssl.CERT_NONE,
                tls_version=getattr(ssl, f"PROTOCOL_{tls_config.get('version', 'TLSv1_2')}")
            )

        # Setup callbacks
        client.on_connect = self._on_mqtt_connect
        client.on_disconnect = self._on_mqtt_disconnect
        client.on_publish = self._on_mqtt_publish
        client.on_message = self._on_mqtt_message

        # Setup Last Will and Testament
        base_topic = mqtt_config.get('base_topic', 'homeassistant/sensor/nas')
        client.will_set(f"{base_topic}/availability", "offline", qos=1, retain=True)

        return client

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        """MQTT connection callback."""
        if reason_code == 0:
            self.logger.info("Connected to MQTT broker")
            self._publish_availability("online")

            # Subscribe to fan control command topic
            base_topic = self.config['mqtt'].get('base_topic', 'homeassistant/sensor/nas')
            command_topic = f"{base_topic}/fan/set"
            client.subscribe(command_topic, qos=1)
            self.logger.info(f"Subscribed to fan control topic: {command_topic}")
        else:
            self.logger.error(f"Failed to connect to MQTT broker: {reason_code}")

    def _on_mqtt_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """MQTT disconnection callback."""
        if reason_code != 0:
            self.logger.warning("Unexpected MQTT disconnection")
        else:
            self.logger.info("Disconnected from MQTT broker")

    def _on_mqtt_publish(self, client, userdata, mid, reason_code, properties):
        """MQTT publish callback."""
        pass  # Could add debug logging here if needed

    def _on_mqtt_message(self, client, userdata, msg):
        """MQTT message callback for fan control commands."""
        try:
            base_topic = self.config['mqtt'].get('base_topic', 'homeassistant/sensor/nas')
            command_topic = f"{base_topic}/fan/set"

            if msg.topic == command_topic:
                command = msg.payload.decode('utf-8').strip()
                self.logger.info(f"Received fan control command: {command}")

                # Map "Auto" to "normal" for user-friendly naming
                if command.lower() == "auto":
                    command = "normal"

                # Execute fan speed change
                success = self.set_fan_speed(command)

                if success:
                    # Update the current fan mode state
                    display_mode = "Auto" if command == "normal" else command
                    self.current_fan_mode = display_mode
                    self._publish_mqtt(f"{base_topic}/fan/state", display_mode)

        except Exception as e:
            self.logger.error(f"Error processing MQTT message: {e}")

    def _publish_mqtt(self, topic: str, payload: str, retain: bool = True, qos: int = 1):
        """Publish message to MQTT broker."""
        try:
            result = self.mqtt_client.publish(topic, payload, qos=qos, retain=retain)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                self.logger.warning(f"Failed to publish to {topic}: {result.rc}")
        except Exception as e:
            self.logger.error(f"Error publishing to MQTT: {e}")

    def _publish_availability(self, status: str):
        """Publish availability status."""
        base_topic = self.config['mqtt'].get('base_topic', 'homeassistant/sensor/nas')
        self._publish_mqtt(f"{base_topic}/availability", status)

    def _execute_ssh_command(self, command: str, allow_nonzero_exit: bool = False) -> Optional[str]:
        """Execute SSH command with retry logic."""
        nas_config = self.config['nas']
        ssh_config = self.config.get('ssh', {})

        max_retries = ssh_config.get('max_retries', 3)
        timeout = ssh_config.get('timeout', 30)
        retry_delay = ssh_config.get('retry_delay', 5)

        ssh_cmd = [
            'ssh',
            '-o', f"ConnectTimeout={ssh_config.get('connect_timeout', 10)}",
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no',
            f"{nas_config['username']}@{nas_config['hostname']}",
            command
        ]

        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Executing SSH command (attempt {attempt + 1}): {command}")
                result = subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    check=False if allow_nonzero_exit else True
                )

                # If we allow non-zero exit codes, return output regardless
                if allow_nonzero_exit:
                    return result.stdout.strip()

                # Otherwise, check if command succeeded
                if result.returncode == 0:
                    return result.stdout.strip()
                else:
                    raise subprocess.CalledProcessError(result.returncode, ssh_cmd, result.stdout, result.stderr)

            except subprocess.TimeoutExpired:
                self.logger.warning(f"SSH command timed out (attempt {attempt + 1}): {command}")
            except subprocess.CalledProcessError as e:
                self.logger.warning(f"SSH command failed (attempt {attempt + 1}): {command} - {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error executing SSH command: {e}")

            if attempt < max_retries - 1:
                self.logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

        self.logger.error(f"SSH command failed after {max_retries} attempts: {command}")
        return None

    def _parse_disk_usage(self, df_output: str, disk_name: str) -> Dict[str, Optional[int]]:
        """Parse disk usage from df command output."""
        try:
            # Find the line containing the disk name
            for line in df_output.split('\n'):
                if disk_name in line:
                    # Clean up multiple spaces and split
                    parts = re.sub(r'\s+', ' ', line.strip()).split()
                    if len(parts) >= 5:
                        return {
                            'total': int(parts[1]) if parts[1].isdigit() else None,
                            'used': int(parts[2]) if parts[2].isdigit() else None,
                            'available': int(parts[3]) if parts[3].isdigit() else None,
                            'percentage': int(parts[4].rstrip('%')) if parts[4].rstrip('%').isdigit() else None
                        }
        except Exception as e:
            self.logger.error(f"Error parsing disk usage for {disk_name}: {e}")

        return {'total': None, 'used': None, 'available': None, 'percentage': None}

    def _parse_temperature_data(self, fc_output: str) -> Dict[str, Optional[int]]:
        """Parse temperature data from fan_control output."""
        temps = {}
        try:
            for line in fc_output.split('\n'):
                line = line.strip()
                if 'Current temperature is' in line:
                    # Extract temperature from "Current temperature is 44"
                    parts = line.split()
                    if len(parts) >= 4 and parts[3].isdigit():
                        temps['board'] = int(parts[3])
                elif 'hd0 temperature=' in line:
                    # Extract temperature from "hd0 temperature=41"
                    temp_str = line.split('=')[1].strip()
                    if temp_str.isdigit():
                        temps['hd0'] = int(temp_str)
                elif 'hd1 temperature=' in line:
                    # Extract temperature from "hd1 temperature=42"
                    temp_str = line.split('=')[1].strip()
                    if temp_str.isdigit():
                        temps['hd1'] = int(temp_str)
                elif 'CPU temperature=' in line:
                    # Extract temperature from "CPU temperature=69"
                    temp_str = line.split('=')[1].strip()
                    if temp_str.isdigit():
                        temps['cpu'] = int(temp_str)
        except Exception as e:
            self.logger.error(f"Error parsing temperature data: {e}")

        return temps

    def _parse_ups_data(self, ups_output: str) -> Dict[str, Optional[str]]:
        """Parse UPS data from upsc output."""
        ups_data = {}
        raw_data = {}

        try:
            for line in ups_output.split('\n'):
                line = line.strip()
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()

                    if key == 'battery.charge':
                        ups_data['batterycharge'] = float(value) if value.replace('.', '').isdigit() else None
                    elif key == 'battery.voltage':
                        ups_data['batteryvoltage'] = float(value) if value.replace('.', '').isdigit() else None
                    elif key == 'input.voltage':
                        ups_data['inputvoltage'] = float(value) if value.replace('.', '').isdigit() else None
                    elif key == 'ups.load':
                        ups_data['load'] = float(value) if value.replace('.', '').isdigit() else None
                        raw_data['load'] = ups_data['load']
                    elif key == 'ups.realpower.nominal':
                        raw_data['nominal_power'] = float(value) if value.replace('.', '').isdigit() else None
                    elif key == 'ups.status':
                        ups_data['status'] = value

            # Calculate real power: (load / 100) * nominal_power
            if raw_data.get('load') is not None and raw_data.get('nominal_power') is not None:
                ups_data['realpower'] = (raw_data['load'] / 100) * raw_data['nominal_power']
            else:
                ups_data['realpower'] = None

        except Exception as e:
            self.logger.error(f"Error parsing UPS data: {e}")

        return ups_data

    def _check_firmware_update(self) -> Dict[str, str]:
        """Check latest firmware online and current version via SSH."""
        result = {"current": "unknown", "latest": "unknown", "status": "error"}
        try:
            # Get current firmware version from NAS
            local_version = self._execute_ssh_command("cat /etc/version || cat /usr/local/config/version")
            if local_version:
                result["current"] = local_version.strip()

            # Get latest firmware version from WD site
            url = "https://support-en.wd.com/app/products/product-detailweb/p/130"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                match = re.search(r"Current Firmware.*?(\d+\.\d+\.\d+)", resp.text)
                if match:
                    result["latest"] = match.group(1)

            # Determine status
            if result["current"] == "unknown" or result["latest"] == "unknown":
                result["status"] = "error"
            elif result["current"] == result["latest"]:
                result["status"] = "up-to-date"
            else:
                result["status"] = "update available"

        except Exception as e:
            self.logger.error(f"Error checking firmware: {e}")
            result["status"] = "error"

        return result

    def set_fan_speed(self, speed: str) -> bool:
        """
        Set the fan speed of the NAS.

        Args:
            speed: Either "normal" to resume automatic fan control,
                   or a number between 0-6 to set manual fan speed

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Setting fan speed to: {speed}")

        if speed == "normal":
            # Resume normal operation by starting the WD daemon process
            # First check the status
            status_output = self._execute_ssh_command("/etc/init.d/wdtmsd status", allow_nonzero_exit=True)

            # If status command failed or service isn't running, start it
            if status_output is None or "not running" in status_output.lower():
                self.logger.info("Starting wdtmsd service for automatic fan control")
                result = self._execute_ssh_command("/etc/init.d/wdtmsd start")
                if result is not None:
                    self.logger.info("Fan control resumed to normal automatic mode")
                    return True
                else:
                    self.logger.error("Failed to start wdtmsd service")
                    return False
            else:
                self.logger.info("wdtmsd service already running")
                return True

        elif speed.isdigit() and 0 <= int(speed) <= 6:
            # Set manual fan speed
            self.logger.info(f"Setting manual fan speed to level {speed}")

            # Stop the WD daemon to take manual control
            stop_result = self._execute_ssh_command("/etc/init.d/wdtmsd stop", allow_nonzero_exit=True)
            if stop_result is None:
                self.logger.warning("Failed to stop wdtmsd service (may already be stopped)")

            # Set the fan speed
            fan_result = self._execute_ssh_command(f"fan_control -f {speed}")
            if fan_result is not None:
                self.logger.info(f"Fan speed set to level {speed}")
                return True
            else:
                self.logger.error(f"Failed to set fan speed to {speed}")
                return False
        else:
            self.logger.error(f"Invalid fan speed: {speed}. Must be 'normal' or 0-6")
            return False

    def _publish_discovery_messages(self):
        """Publish Home Assistant MQTT Discovery messages."""
        self.logger.info("Publishing MQTT discovery messages...")

        mqtt_config = self.config['mqtt']
        discovery_prefix = mqtt_config.get('discovery_prefix', 'homeassistant')
        base_topic = mqtt_config.get('base_topic', 'homeassistant/sensor/nas')

        # Device information
        device_info = {
            "identifiers": ["nas_wdmycloud"],
            "name": self.config['nas'].get('device_name', 'WD MyCloud Ex2 Ultra'),
            "manufacturer": "Western Digital",
            "model": "Ex2 Ultra"
        }

        # Define all sensors
        sensors = {
            # Internal disk
            'hdtotal': {'name': 'NAS HD Total', 'unit': 'MB', 'device_class': 'data_size', 'state_class': 'total'},
            'hdused': {'name': 'NAS HD Used', 'unit': 'MB', 'device_class': 'data_size', 'state_class': 'total'},
            'hdavailable': {'name': 'NAS HD Available', 'unit': 'MB', 'device_class': 'data_size', 'state_class': 'total'},
            'hdinuseperc': {'name': 'NAS HD Usage %', 'unit': '%', 'state_class': 'measurement'},

            # USB disk
            'usbtotal': {'name': 'NAS USB Total', 'unit': 'MB', 'device_class': 'data_size', 'state_class': 'total'},
            'usbused': {'name': 'NAS USB Used', 'unit': 'MB', 'device_class': 'data_size', 'state_class': 'total'},
            'usbavailable': {'name': 'NAS USB Available', 'unit': 'MB', 'device_class': 'data_size', 'state_class': 'total'},
            'usbinuseperc': {'name': 'NAS USB Usage %', 'unit': '%', 'state_class': 'measurement'},

            # Temperatures
            'boardtemperature': {'name': 'NAS Board Temperature', 'unit': '°C', 'device_class': 'temperature', 'state_class': 'measurement'},
            'hd0temperature': {'name': 'NAS HD0 Temperature', 'unit': '°C', 'device_class': 'temperature', 'state_class': 'measurement'},
            'hd1temperature': {'name': 'NAS HD1 Temperature', 'unit': '°C', 'device_class': 'temperature', 'state_class': 'measurement'},
            'cputemperature': {'name': 'NAS CPU Temperature', 'unit': '°C', 'device_class': 'temperature', 'state_class': 'measurement'},

            # Fan
            'fanrpm': {'name': 'NAS Fan RPM', 'unit': 'rpm', 'state_class': 'measurement'},

            # Update status
            'updatestatus': {'name': 'NAS Update Status'},
            'current_firmware': {'name': 'NAS Current Firmware'},
            'latest_firmware': {'name': 'NAS Latest Firmware'},

            # UPS
            'upsstatus': {'name': 'UPS Status'},
            'upsbatterycharge': {'name': 'UPS Battery Charge', 'unit': '%', 'device_class': 'battery', 'state_class': 'measurement'},
            'upsbatteryvoltage': {'name': 'UPS Battery Voltage', 'unit': 'V', 'device_class': 'voltage', 'state_class': 'measurement'},
            'upsinputvoltage': {'name': 'UPS Input Voltage', 'unit': 'V', 'device_class': 'voltage', 'state_class': 'measurement'},
            'upsload': {'name': 'UPS Load', 'unit': '%', 'state_class': 'measurement'},
            'upsrealpower': {'name': 'UPS Real Power', 'unit': 'W', 'device_class': 'power', 'state_class': 'measurement'},
        }

        # Publish discovery message for each sensor
        for sensor_id, sensor_config in sensors.items():
            discovery_topic = f"{discovery_prefix}/sensor/nas_{sensor_id}/config"

            config_payload = {
                "name": sensor_config['name'],
                "state_topic": f"{base_topic}/{sensor_id}",
                "availability_topic": f"{base_topic}/availability",
                "unique_id": f"nas_{sensor_id}",
                "device": device_info
            }

            # Add optional attributes
            if 'unit' in sensor_config:
                config_payload['unit_of_measurement'] = sensor_config['unit']
            if 'device_class' in sensor_config:
                config_payload['device_class'] = sensor_config['device_class']
            if 'state_class' in sensor_config:
                config_payload['state_class'] = sensor_config['state_class']

            self._publish_mqtt(discovery_topic, json.dumps(config_payload))

        # Publish discovery for fan control select entity
        fan_discovery_topic = f"{discovery_prefix}/select/nas_fan_control/config"
        fan_config = {
            "name": "NAS Fan Control",
            "command_topic": f"{base_topic}/fan/set",
            "state_topic": f"{base_topic}/fan/state",
            "availability_topic": f"{base_topic}/availability",
            "unique_id": "nas_fan_control",
            "options": ["Auto", "0", "1", "2", "3", "4", "5", "6"],
            "icon": "mdi:fan",
            "device": device_info
        }
        self._publish_mqtt(fan_discovery_topic, json.dumps(fan_config))

        self.logger.info("Discovery messages published")

    def _collect_and_publish_data(self) -> bool:
        """Collect data from NAS and publish to MQTT."""
        self.logger.info("Starting data collection...")

        nas_config = self.config['nas']
        base_topic = self.config['mqtt'].get('base_topic', 'homeassistant/sensor/nas')

        success = True

        try:
            # Collect disk usage
            self.logger.debug("Collecting disk usage data...")
            df_output = self._execute_ssh_command("df -m")
            if df_output:
                # Parse internal HDD
                hd_data = self._parse_disk_usage(df_output, nas_config['internal_disk'])
                if any(v is not None for v in hd_data.values()):
                    for key, value in hd_data.items():
                        if value is not None:
                            topic_key = f"hd{key}" if key != 'percentage' else 'hdinuseperc'
                            self._publish_mqtt(f"{base_topic}/{topic_key}", str(value))

                # Parse USB HDD
                usb_data = self._parse_disk_usage(df_output, nas_config['usb_disk'])
                if any(v is not None for v in usb_data.values()):
                    for key, value in usb_data.items():
                        if value is not None:
                            topic_key = f"usb{key}" if key != 'percentage' else 'usbinuseperc'
                            self._publish_mqtt(f"{base_topic}/{topic_key}", str(value))
            else:
                success = False

            # Collect temperature data
            self.logger.debug("Collecting temperature data...")
            temp_output = self._execute_ssh_command("fan_control -g 0")
            if temp_output:
                temps = self._parse_temperature_data(temp_output)
                for key, value in temps.items():
                    if value is not None:
                        topic_key = f"{key}temperature"
                        self._publish_mqtt(f"{base_topic}/{topic_key}", str(value))
            else:
                success = False

            # Collect fan RPM
            self.logger.debug("Collecting fan RPM...")
            fan_output = self._execute_ssh_command("fan_control -g 4")
            if fan_output and '=' in fan_output:
                try:
                    fan_rpm = fan_output.split('=')[1].strip()
                    if fan_rpm.isdigit():
                        self._publish_mqtt(f"{base_topic}/fanrpm", fan_rpm)
                except:
                    pass

            # Collect update status
            self.logger.debug("Collecting update status...")
            fw = self._check_firmware_update()
            self._publish_mqtt(f"{base_topic}/updatestatus", fw["status"])
            self._publish_mqtt(f"{base_topic}/current_firmware", fw["current"])
            self._publish_mqtt(f"{base_topic}/latest_firmware", fw["latest"])

            # Collect UPS data
            self.logger.debug("Collecting UPS data...")
            ups_output = self._execute_ssh_command("upsc usbhid")
            if ups_output:
                ups_data = self._parse_ups_data(ups_output)
                for key, value in ups_data.items():
                    if value is not None:
                        topic_key = f"ups{key}"
                        self._publish_mqtt(f"{base_topic}/{topic_key}", str(value))

            # Check and publish current fan control status
            self.logger.debug("Checking fan control status...")
            status_output = self._execute_ssh_command("/etc/init.d/wdtmsd status", allow_nonzero_exit=True)
            if status_output and "running" in status_output.lower():
                self.current_fan_mode = "Auto"
            else:
                # If wdtmsd is not running, try to determine manual fan speed
                # This is a best-effort attempt; the actual speed might not be detectable
                if self.current_fan_mode == "unknown":
                    self.current_fan_mode = "Manual"

            self._publish_mqtt(f"{base_topic}/fan/state", self.current_fan_mode)

            # If we got here and had some success, mark as online
            self._publish_availability("online")
            # Publish last update timestamp
            self._publish_mqtt(f"{base_topic}/last_update",
                             datetime.now().astimezone().isoformat())

            if success:
                self.logger.info("Data collection completed successfully")
            else:
                self.logger.warning("Data collection completed with some failures")

        except Exception as e:
            self.logger.error(f"Error during data collection: {e}")
            self._publish_availability("offline")
            success = False

        return success

    def run_once(self):
        """Run data collection once."""
        self.mqtt_client = self._setup_mqtt()

        try:
            mqtt_config = self.config['mqtt']
            self.mqtt_client.connect(
                mqtt_config['host'],
                mqtt_config.get('port', 1883),
                mqtt_config.get('keepalive', 60)
            )
            self.mqtt_client.loop_start()

            # Wait a moment for connection
            time.sleep(2)

            success = self._collect_and_publish_data()

            # Wait for messages to be published
            time.sleep(2)

            return success

        except Exception as e:
            self.logger.error(f"Error in run_once: {e}")
            return False
        finally:
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()

    def run_continuous(self):
        """Run continuous monitoring."""
        self.running = True
        self.mqtt_client = self._setup_mqtt()

        try:
            mqtt_config = self.config['mqtt']
            self.mqtt_client.connect(
                mqtt_config['host'],
                mqtt_config.get('port', 1883),
                mqtt_config.get('keepalive', 60)
            )
            self.mqtt_client.loop_start()

            # Wait for connection
            time.sleep(2)

            # Publish discovery messages
            self._publish_discovery_messages()

            poll_interval = self.config.get('monitoring', {}).get('poll_interval', 300)

            self.logger.info(f"Starting continuous monitoring (interval: {poll_interval}s)")

            while self.running:
                try:
                    success = self._collect_and_publish_data()
                    if not success:
                        self._publish_availability("offline")

                    # Sleep in small intervals to allow for graceful shutdown
                    for _ in range(poll_interval):
                        if not self.running:
                            break
                        time.sleep(1)

                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.logger.error(f"Error in monitoring loop: {e}")
                    time.sleep(30)  # Wait before retrying

        except Exception as e:
            self.logger.error(f"Error in run_continuous: {e}")
        finally:
            self.logger.info("Shutting down...")
            if self.mqtt_client:
                self._publish_availability("offline")
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()

    def setup_discovery(self):
        """Setup MQTT discovery only."""
        self.mqtt_client = self._setup_mqtt()

        try:
            mqtt_config = self.config['mqtt']
            self.mqtt_client.connect(
                mqtt_config['host'],
                mqtt_config.get('port', 1883),
                mqtt_config.get('keepalive', 60)
            )
            self.mqtt_client.loop_start()

            # Wait for connection
            time.sleep(2)

            self._publish_discovery_messages()

            # Wait for messages to be published
            time.sleep(2)

        finally:
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='NAS MQTT Monitor with Fan Control')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode')
    parser.add_argument('--setup', action='store_true', help='Setup MQTT discovery only')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--fan', type=str, metavar='SPEED',
                       help='Set fan speed: "normal" for automatic control, or 0-6 for manual speed')

    args = parser.parse_args()

    try:
        monitor = NASMonitor(args.config)

        if args.fan:
            # Fan control mode
            success = monitor.set_fan_speed(args.fan)
            sys.exit(0 if success else 1)
        elif args.setup:
            monitor.setup_discovery()
        elif args.continuous:
            monitor.run_continuous()
        elif args.once:
            success = monitor.run_once()
            sys.exit(0 if success else 1)
        else:
            # Default behavior
            monitor.run_continuous()

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
