#!/bin/bash
# Run the entire football intelligence system

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Football Intelligence Platform${NC}"
echo "============================="

# Create necessary directories
echo -e "${GREEN}Creating required directories...${NC}"
mkdir -p data/daily data/raw sofascore_data/daily sofascore_data/raw

# Check Python installation
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 is not installed. Please install Python 3.8 or higher.${NC}"
    exit 1
fi

# Check dependencies
echo -e "${GREEN}Checking dependencies...${NC}"
pip install -q -r requirements.txt
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to install dependencies. Please check your pip installation.${NC}"
    exit 1
fi

# Ask for date range
echo -e "${GREEN}How many days of data would you like to collect?${NC} (default: 7)"
read days_input
days=${days_input:-7}

# Run the scraper
echo -e "${GREEN}Collecting football match data for the next $days days...${NC}"
python main.py --days $days --stats
if [ $? -ne 0 ]; then
    echo -e "${RED}Data collection failed. See error messages above.${NC}"
    exit 1
fi

echo -e "${GREEN}Data collection complete!${NC}"

# Ask to launch dashboard
echo -e "${GREEN}Would you like to launch the dashboard now?${NC} (y/n)"
read launch_dashboard

if [[ $launch_dashboard == "y" || $launch_dashboard == "Y" ]]; then
    echo -e "${GREEN}Launching dashboard...${NC}"
    python run_dashboard.py
    echo -e "${GREEN}Dashboard closed.${NC}"
else
    echo -e "${YELLOW}You can run the dashboard later with: python run_dashboard.py${NC}"
fi

echo -e "${GREEN}All operations completed successfully!${NC}"