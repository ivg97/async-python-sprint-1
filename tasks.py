import json

from multiprocessing import Process
from multiprocessing import Queue

from external.client import YandexWeatherAPI
from utils import get_url_by_city_name, FILE_NAME
from external.analyzer import analyze_json
from logger import logger


class DataFetchingTask:
    """Получение данных через API"""

    @staticmethod
    def get_info_by_city(city: str) -> dict:
        logger.info(f"Начало выгрузки погоды города {city}")
        url_with_data = get_url_by_city_name(city)
        try:
            response = YandexWeatherAPI.get_forecasting(url_with_data)
            if response:
                response["city"] = city
                logger.info(f'Получили погоду для города {city}')
                return response
        except Exception as err:
            logger.error(f"Ошибка получения данных города {city}. Error: {err}")
        return dict()


class DataCalculationTask:
    """Вычисление погодных параметров"""

    def __init__(self, queue: Queue):
        self._queue = queue

    def run_parsing(self, data: dict) -> None:
        if data:
            logger.info(f"Вычисляем погодные параметры...")
            forecasts = analyze_json(data)

            # Вычисление средней температуры и среднего количества дней без осадков
            avg_temp = 0.0
            avg_precipitation = 0.0
            days_avg_temp = 0

            for day in forecasts["days"]:
                if isinstance(day["temp_avg"], float):
                    avg_temp += day["temp_avg"]
                    days_avg_temp += 1
                avg_precipitation += day["relevant_cond_hours"]
            avg_temp /= days_avg_temp
            avg_precipitation /= len(forecasts["days"])

            logger.info(f"Завершили вычисление погодных парамеров")
            self._queue.put(
                {
                    "city": data["city"],
                    "days": forecasts["days"],
                    "avg_temperature": avg_temp,
                    "avg_hours_without_precipitation": avg_precipitation,
                    "rating": None,
                }
            )
            logger.info("Записали результаты вычислений в очередь")
        else:
            logger.error(f"Ошибка при вычислении погодных параметров. Нет данных: {data}")


class DataAggregationTask(Process):
    """Объединение вычисленных данных"""

    def __init__(self, queue: Queue):
        super().__init__()
        self._queue = queue

    def run(self) -> None:
        result = list()
        while True:
            if self._queue.empty():
                logger.info("Очередь пуста!")
                break
            else:
                data = self._queue.get()
                result.append(data)
                logger.info(f"Получили и обработали данные из очереди {data}")

        with open(FILE_NAME, "w") as file:
            logger.info(f"Записываем данные в файл {FILE_NAME}: {result}")
            result = json.dumps(result, indent=4)
            file.write(result)


class DataAnalyzingTask:
    """Финальный анализ и получение результата"""

    @staticmethod
    def finish_analyze_data() -> None:
        with open(FILE_NAME, "r") as file:
            data = json.loads(file.read())

        logger.info("Приступаем к анализу данных...")
        city_avg_values = [
            (
                city["city"],
                city["avg_temperature"],
                city["avg_hours_without_precipitation"],
            ) for city in data
        ]
        city_avg_values.sort(key=lambda x: (x[1], x[2]), reverse=True)
        city_rating = dict()
        for i in range(len(city_avg_values)):
            city_rating[city_avg_values[i][0]] = i + 1
        for city in data:
            city["rating"] = city_rating[city["city"]]
        with open(FILE_NAME, "w") as file:
            logger.info(f"Завершили анализ, записываем результат в файл: {data}")
            result = json.dumps(data, indent=4)
            file.write(result)
