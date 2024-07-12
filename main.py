"""
Модуль запуска программы
"""
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager
from multiprocessing import Pool
from os import cpu_count

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, FILE_NAME
from logger import logger


def forecast_weather() -> None:
    manager = Manager()
    aggregation_queue = manager.Queue()

    # Получаем погоду городов через API
    fetching_task = DataFetchingTask()

    with ThreadPoolExecutor() as executor:
        weather_data_city = executor.map(
            fetching_task.get_info_by_city, list(CITIES.keys())
        )

    # Вычислем параметры погоды
    calculation_task = DataCalculationTask(queue=aggregation_queue)

    with Pool(processes=cpu_count()) as pool:
        pool.map(calculation_task.run_parsing, weather_data_city)

    # Объединеняем данные
    aggregation_task = DataAggregationTask(queue=aggregation_queue)
    aggregation_task.start()
    aggregation_task.join()

    # Анализ погоды и получение результата
    analyzing_task = DataAnalyzingTask()
    analyzing_task.finish_analyze_data()


if __name__ == "__main__":
    logger.info(f"Начало работы приложения...")
    forecast_weather()
    logger.info(f"Приложение завершено! Результат в файле {FILE_NAME}.")
