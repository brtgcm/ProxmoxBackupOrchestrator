# -*- coding: utf-8 -*-
import subprocess
import time
import yaml
import schedule
import logging
import os
from datetime import datetime

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("backup_orchestrator.log"),
        logging.StreamHandler()
    ]
)

def load_config(config_path):
    """Loads configuration from YAML file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        exit(1)
    except yaml.YAMLError as e:
        logging.error(f"YAML parsing error: {e}")
        exit(1)
    except Exception as e:
        logging.error(f"Unexpected error while loading configuration: {e}")
        exit(1)

def run_vzdump(node_config, config):
    """Runs vzdump on a specific node via SSH."""

    ssh_command = [
        "/usr/bin/ssh",
        "-p", "22",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        f"root@{node_config['fqdn']}",
        "vzdump",
        "--mode", "snapshot",
        "--mailto", config['mailto'],
        "--fleecing", str(config['fleecing']),
        "--bwlimit", str(config['bwlimit']),
        "--storage", config['pbs_storage'],
        "--notes-template", config['notes_template'],
        "--mailnotification", config['mailnotification'],
        "--node", node_config['shortname'],
        "--all", "1"
    ]

    # Excluded VMs handling
    if 'exclude_vms' in node_config:
        exclude_list = node_config['exclude_vms']
    elif 'exclude_vms' in config:
        exclude_list = config['exclude_vms']
    else:
        exclude_list = []  # No global exclusion

    if exclude_list:  # If there are VMs to exclude
        exclude_str = ','.join(map(str, exclude_list))
        ssh_command.extend(["--exclude", exclude_str])

    logging.info(f"Running backup on node: {node_config['shortname']} (via SSH to {node_config['fqdn']})")
    logging.info(f"Command: {' '.join(ssh_command)}")

    try:
        result = subprocess.run(ssh_command, capture_output=True, text=True, check=True, timeout=1200)
        logging.info(f"Backup successfully completed on node {node_config['shortname']}.\nOutput:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during backup of node {node_config['shortname']} (via SSH):")
        logging.error(f"  Return code: {e.returncode}")
        logging.error(f"  Stdout: {e.stdout}")
        logging.error(f"  Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        logging.error("Error: 'ssh' command not found. Make sure it's installed and in the PATH.")
        return False
    except subprocess.TimeoutExpired:
        logging.error(f"Timeout expired while running vzdump on {node_config['shortname']}.")
        return False
    except Exception as e:
        logging.exception(f"Unexpected error while running vzdump on {node_config['shortname']} (via SSH): {e}")
        return False

def backup_job(config):
    """Runs backup sequentially on all nodes."""
    logging.info("Starting backup cycle...")
    start_time = datetime.now()

    for node_config in config['nodes']:
        if not run_vzdump(node_config, config):
            logging.warning(f"Backup of node {node_config.get('shortname', 'N/A')} failed. Continuing with the next nodes.")

    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"Backup cycle completed. Total duration: {duration}")

def main():
    """Main function: loads config and schedules backups."""

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.yaml')

    config = load_config(config_path)
    logging.info(f"Configuration loaded from {config_path}: {config}")

    try:
        parts = config['schedule'].split()
        if len(parts) != 5:
            raise ValueError("The schedule string must have 5 parts")

        minute, hour, day_of_month, month, day_of_week = parts

        if hour == "*" and minute == "*":
            # Run every minute
            schedule.every(1).minutes.do(backup_job, config)
            logging.info("Backup scheduled to run every minute.")

        elif hour == "*":
            # Run every hour at specified minute
            schedule.every().hour.at(f":{minute.zfill(2)}").do(backup_job, config)
            logging.info(f"Backup scheduled to run every hour at minute {minute}.")

        else:
            # Run at a specific hour every day
            schedule.every().day.at(f"{hour.zfill(2)}:{minute.zfill(2)}").do(backup_job, config)
            logging.info(f"Backup scheduled to run daily at {hour.zfill(2)}:{minute.zfill(2)}.")

    except ValueError as e:
        logging.error(f"Invalid 'schedule' format in config.yaml: {config['schedule']}. Expected: 'minute hour day_of_month month day_of_week'. Error: {e}")
        exit(1)

    logging.info("Orchestrator started. Waiting for next scheduled run...")

    time.sleep(5)

    while True:
        try:
            now = datetime.now()
            # Log current time and next scheduled execution
            logging.debug(f"Current time: {now}, Next run: {schedule.next_run}")
            schedule.run_pending()
            # Reduce sleep time for more frequent checks
            time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Orchestrator interrupted by user.")
            break
        except Exception as e:
            logging.exception(f"Unexpected error in main loop: {e}")

if __name__ == "__main__":
    main()
