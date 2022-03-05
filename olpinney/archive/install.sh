PLATFORM=$(python3 -c 'import platform; print(platform.system())')

echo -e "1. Creating new virtual environment..."

python3 -m venv env 

echo -e "2. Installing Requirements..."

source env/bin/activate
pip install -r requirements.txt

echo -e "3. Installing Selenium Drivers..."
if [[ $PLATFORM == 'Linux' ]];  then 
    wget https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz
    tar -xvf geckodriver-v0.30.0-linux64.tar.gz
    rm -rf geckodriver-v0.30.0-linux64.tar.gz 
    mv geckodriver env/bin/ 
fi
deactivate 
echo -e "Install is complete."