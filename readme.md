## Задачи

### Задача 1

- Написать скрипт с двумя соединениями А, Б.
- Для соединения А запустить процесс вставки записей построчно с коммитом каждые 5 секунд,
и подсчетом кол-во записей в таблице каждую секунду.
- Для соединения Б запустить процесс подсчета количества записей в таблице каждую секунду на текущий момент времени.
- Построить график Ось Х - время, Ось У - количество записей в таблице. На одном графике должны отоброжатся данные для соединения А и соединения Б.
- Получить 4 графика для разных типов изоляции.

Форма сдачи python скрипт + 4 png файла графиков, файл с requirements.txt 
Графики должны быть с названием и подписями осей.
Требования к скрипту: 
 - конфиги коннекшена к базе указаны в начале скрипта в одном месте;
 - при запуске скрипт должен генерить требуемые объекты в БД, и 4 файла графиков;
 - по окончанию работы скрипт очищает БД.

### Задача 2 - Конкурентный update

Написать скрипт, который создает таблицу c двумя колонками:
- int - ИД записи;
- text - произвольное текстовое поле;
Кол-во строк в таблице не меньше 1000.

Далее скрипт должен запустить 20 процессов конкурентного update-а таблицы.
Процесс заполняет текстовое поле своим ИД.
Уровень изоляции выставить RepeatableRead.
Добиться, чтобы при апдейте возникала SerializationFailure.
Среднее кол-во SerializationFailure для процессов не менее 10.
ИД для апдейта сгенерить случайным генератором.

По окончании работы скрипта вывести кол-во SerializationFailure для каждого процесса и таблицу PID и количество записей, которые проапдейтил каждый процесс в итоге.
Сделать вывод, насколько равномерно распределились апдейты строк по процессам.

Форма сдачи python скрипт.

## Запуск

Предполагается, что перед запуском сервер PostgreSQL уже запущен. При необходимости можно сконфигурировать параметры подключения в каждом из скриптов.

Для запуска необходимо: 
- установить зависимости `pip install -r requirements.txt`
- создать в корне проекта файл .env и задать в нем логин и пароль для PostgreSQL (см [python-dotenv](https://github.com/theskumar/python-dotenv))
- запустить `python task_1.py` или `python task_2.py` соответственно


### Пример консольного вывода в задаче 2

| PID     | Errors | Updated rows |
|---------|--------|--------------|
| 12604   |   895  |          105 |
| 98372   |   217  |          783 |
| 59404   |   917  |           83 |
| 123796  |   900  |          100 |
| 76104   |   881  |          119 |
| 105672  |   423  |          577 |
| 41196   |   815  |          185 |
| 27056   |   965  |           35 |
| 70180   |   894  |          106 |
| 57304   |   948  |           52 |
| 38608   |   997  |            3 |
| 35440   |   943  |           57 |
| 113556  |   705  |          295 |
| 18180   |   962  |           38 |
| 109256  |   804  |          196 |
| 43132   |   904  |           96 |
| 12896   |   809  |          191 |
| 84540   |   996  |            4 |
| 92180   |   989  |           11 |
| 110676  |   943  |           57 |

Количество обновленных строк между потоками распределено не равномерно

