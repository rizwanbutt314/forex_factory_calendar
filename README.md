### Description:
The purpose of this scraper is to extract the required information from following sites:
* https://www.forexfactory.com/calendar
* https://www.forexfactory.com/calendar?week=next
* https://www.dailyfx.com/economic-calendar#next-seven-days

### PreReqs:
* Python: 3.6+

### Setup:
* create a virtual environment: `virtualenv -p /usr/bin/python3 env` (Optional)
* activate the environemnt: `source ./env/bin/activate` (Optional when you don't need first step)
* install requirements: `pip install -r requirements.txt`
* Edit `utils.py` file to update the Datbase Settings according to yours
* Following are the Database variables which needs to be updated
```
DB_HOST = "localhost"
DB_USERNAME = "testUsername"
DB_PASSWORD = "testPassword"
DB_DATABASE = "testDatabase"
DB_TABLE = "testTable"
```

### Run:
* Command to run scraper: `python main.py`

### Note:
*  `requirements.txt` file contains the list of packages that are required to install.
* Extracted information will be saved in file: `output.csv`
* Plus the information will be saved in a MySQL database too.