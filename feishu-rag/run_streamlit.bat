@echo off
cd /d "%~dp0"
streamlit run app_streamlit.py --server.port 8501
pause
