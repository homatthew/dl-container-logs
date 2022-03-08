# Setup
Run kinit in your shell to authorize the script to access hadoop logs on your behalf
```
kinit
```
Set up your virtual environment / dependencies inside the repo. This is a one time operation
```
git clone https://github.com/homatthew/dl-container-logs.git
cd dl-container-logs
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Every following time, you can enter the venv using without running the above commands:
```
source .venv/bin/activate
```

While in the venv, you can run the script using
```
python dl-container-logs.py <TRACKING_URL>
```

After you are done, exit the python virtual env using
```
deactivate
```