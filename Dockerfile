FROM python:3.7
EXPOSE 8501
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip3 install -r requirements.txt
COPY ./streamlit .
CMD streamlit run streamlitapp.py