import subprocess
import logging
import sys
from pathlib import Path

def setup_logging():
    logger = logging.getLogger('FirewallSetup')
    logger.setLevel(logging.DEBUG)
    
    handler = logging.FileHandler('firewall_setup.log')
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    
    return logger

def create_firewall_rules():
    logger = setup_logging()
    
    rules = [
        {
            'name': 'Python DLNA Server - HTTP',
            'port': 8000,
            'protocol': 'TCP'
        },
        {
            'name': 'Python DLNA Server - SSDP UDP',
            'port': 1900,
            'protocol': 'UDP'
        }
    ]
    
    try:
        # Check existing rules
        check_cmd = 'netsh advfirewall firewall show rule name="Python DLNA Server - HTTP"'
        result = subprocess.run(check_cmd, capture_output=True, text=True)
        
        if "No rules match the specified criteria" not in result.stdout:
            logger.info("Firewall rules already exist")
            return True
            
        logger.info("Creating Windows Firewall rules...")
        
        for rule in rules:
            cmd = [
                'netsh', 'advfirewall', 'firewall', 'add', 'rule',
                f'name="{rule["name"]}"',
                'dir=in',
                'action=allow',
                f'protocol={rule["protocol"]}',
                f'localport={rule["port"]}',
                'profile=private',
                'enable=yes',
                'description="Python DLNA Server for streaming media"'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Created firewall rule: {rule['name']}")
            else:
                logger.error(f"Failed to create rule: {rule['name']}")
                logger.error(f"Error: {result.stderr}")
                return False
        
        # Enable network discovery
        try:
            logger.info("Enabling Network Discovery...")
            
            # Enable required services
            services = ['fdPHost', 'FDResPub']
            for service in services:
                subprocess.run(['sc', 'config', service, 'start=auto'], check=True)
                subprocess.run(['net', 'start', service], check=False)  # Don't check result as service might be running
                
            logger.info("Network Discovery enabled")
            return True
            
        except Exception as e:
            logger.error(f"Failed to enable Network Discovery: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Error configuring firewall: {str(e)}")
        return False

if __name__ == '__main__':
    success = create_firewall_rules()
    sys.exit(0 if success else 1)
