import asyncio
import aiohttp
import json
from datetime import datetime

API_1 = "https://vpic.nhtsa.dot.gov/api/vehicles/GetAllMakes?format=json"
API_2 = "https://vpic.nhtsa.dot.gov/api/vehicles/GetMakesForVehicleType/car?format=json"

MAX_CONCURRENT_REQUESTS = 3
REQUEST_TIMEOUT = 5
MAX_RETRIES = 2
OUTPUT_FILE = "cars_data.json"

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


async def fetch_data(session, url):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                response = await asyncio.wait_for(session.get(url), timeout=REQUEST_TIMEOUT)
                async with response:
                    if response.status == 200:
                        text = await response.text()
                        text = text.strip()
                        if text.startswith("?"):
                            text = text[text.index("(") + 1:text.rindex(")")]
                        return json.loads(text)
                    print(f"Ошибка {response.status} при запросе {url}")
                    return None
            except asyncio.TimeoutError:
                print(f"Таймаут при запросе {url} (попытка {attempt})")
            except aiohttp.ClientConnectionError:
                print(f"Нет соединения при запросе {url} (попытка {attempt})")
            except (json.JSONDecodeError, ValueError):
                print(f"инвалидный  жсон при запросе {url}")
                return None
            except Exception as e:
                print(f"Ошибка {type(e).__name__}: {e} при запросе {url}")
                return None
            await asyncio.sleep(attempt)
    return None


async def collect_cars_data():
    async with aiohttp.ClientSession() as session:
        task_all_makes = fetch_data(session, API_1)
        task_car_makes = fetch_data(session, API_2)
        results = await asyncio.gather(task_all_makes, task_car_makes)

    all_makes_data = results[0]
    car_makes_data = results[1]

    timestamp = datetime.now().isoformat(timespec="seconds")
    
    record = {
        "timestamp": timestamp,
        "all_makes": [m.get("Make_Name") for m in all_makes_data.get("Results", [])] if all_makes_data else None,
        "passenger_cars": [m.get("MakeName") for m in car_makes_data.get("Results", [])] if car_makes_data else None,
    }
    return record


def save_data(data, filename=OUTPUT_FILE):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")
    print(f"Данные сохранены в '{filename}' (отметка времени: {data['timestamp']})")


async def main():
    print(f"Сбор данных: {datetime.now():%H:%M:%S}")
    try:
        data = await collect_cars_data()
        save_data(data)
        print(f"Всего марок в базе (API_1): {len(data['all_makes']) if data['all_makes'] else 0}")
        print(f"Из них легковых авто (API_2): {len(data['passenger_cars']) if data['passenger_cars'] else 0}")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    



if __name__ == "__main__":
    asyncio.run(main())
