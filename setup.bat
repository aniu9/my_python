@echo off
python.exe -m pip install --upgrade pip
python -m venv myenv
call myenv\Scripts\activate.bat
pip install -r requirements.txt
echo Setup complete.
pause
