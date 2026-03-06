# пока мы здесь
from fastapi import FastAPI, HTTPException
import pandas as pd
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

app = FastAPI()
CSV_URL = "https://raw.githubusercontent.com/koleksuha-maker/homework-3/refs/heads/main/RU_Electricity_Market_PZ_dayahead_price_volume%20(1).csv"
# 1. Функция загрузки данных
def load_data():
    # Мы используем try-except, потому что в задании это обязательное требование
    try:
        # Сначала просто пытаемся открыть файл на компьютере
        df = pd.read_csv("data.csv")
        return df
    except:
        # Если файла "data.csv" нет, эта часть кода сработает и скачает его из интернета
        df = pd.read_csv(CSV_URL)
        # Добавляем колонку id, так как в твоем файле её изначально нет
        df["id"] = range(len(df))
        # Сохраняем, чтобы в следующий раз файл уже был на компьютере
        df.to_csv("data.csv", index=False)
        return df

# 2. Функция сохранения данных
def save_data(df):
    try:
        df.to_csv("data.csv", index=False)
    except:
        # Если вдруг файл нельзя перезаписать, сервер не упадет, а выдаст ошибку
        raise HTTPException(status_code=500, detail="Не удалось сохранить файл")

# 3. Валидация (описание того, какие данные мы принимаем)
class RecordCreate(BaseModel):
    id: Optional[int] = None
    timestep: datetime  # Проверка, что дата введена корректно
    consumption_eur: float
    consumption_sib: float
    price_eur: float
    price_sib: float

# --- Твои эндпоинты (задания 1, 2, 3) ---

@app.get("/records")
def get_records():
    df = load_data()
    return df.to_dict(orient="records")

@app.post("/records")
def add_record(record: RecordCreate):
    df = load_data()
    
    # Считаем ID для новой записи
    if df.empty:
        new_id = 0
    else:
        new_id = int(df["id"].max() + 1)
    
    new_row = record.dict()
    new_row["id"] = new_id
    
    # Склеиваем старые данные с новой строкой
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    save_data(df)
    return new_row

@app.delete("/records/{record_id}")
def delete_record(record_id: int):
    df = load_data()
    
    # Проверка: есть ли такой ID?
    if record_id not in df["id"].values:
        raise HTTPException(status_code=404, detail="Запись с таким ID не найдена")
    
    # Оставляем все строки, кроме той, которую хотим удалить
    df = df[df["id"] != record_id]
    save_data(df)
    return {"message": f"Запись {record_id} удалена"}