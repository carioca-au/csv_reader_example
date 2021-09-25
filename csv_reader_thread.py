import csv
import os
import datetime
import threading
import time

FILE_DIR = os.path.dirname(os.path.realpath(__file__))


def read_csv(file_name):
    for row in open(os.path.join(FILE_DIR, 'data', file_name), "r"):
        yield row


def build_values(row, header=[]):
    values = row.rstrip('\n').split(',')

    if len(values) < 18:
        raise Exception('There are missing values')

    if not header:
        return values

    return {header[index]: values[index] for index in range(0, len(header))}


def calculate_distance(file_name, results):
    try:
        header = []
        for row in read_csv(file_name=file_name):

            if not header:
                header = build_values(row, header)
                continue

            values_dict = build_values(row, header)

            # initiates an entry per car
            if not results.get(values_dict.get('car_number')):
                results[values_dict.get('car_number')] = {}

            car_number = values_dict.get('car_number')
            distance = values_dict.get('distance_m')

            calculated = results.get(car_number, {}).get('distance', 0)
            results[car_number].update(
                {
                    'distance': float(calculated) + float(distance)
                }
            )
        return results
    except Exception as error:
        raise error


def calculate_total_operating_time(file_name, results):
    try:
        header = []
        for row in read_csv(file_name=file_name):

            if not header:
                header = build_values(row, header)
                continue

            values_dict = build_values(row, header)

            # initiates an entry per car
            if not results.get(values_dict.get('car_number')):
                results[values_dict.get('car_number')] = {}

            car_number = values_dict.get('car_number')
            duration_s = values_dict.get('duration_s')

            calculated = results.get(car_number, {}).get('duration_s', 0)
            duration_s = float(calculated) + float(duration_s)
            duration_h = str(datetime.timedelta(seconds=duration_s))
            duration_days = str(datetime.timedelta(seconds=duration_s).days)

            results[car_number].update(
                {
                    'duration_s': duration_s,
                    'duration_h': duration_h,
                    'duration_days': duration_days
                }

            )

        return results

    except Exception as error:
        raise error


def calculate_utilization(file_name, results):
    try:
        header = []
        for row in read_csv(file_name=file_name):

            if not header:
                header = build_values(row, header)
                continue

            values_dict = build_values(row, header)

            # initiates an entry per car
            if not results.get(values_dict.get('car_number')):
                results[values_dict.get('car_number')] = {}

            car_number = values_dict.get('car_number')
            actime_time = values_dict.get('ignition_on')

            calculated = results.get(car_number, {}).get('actime_time', 0)
            actime_time = float(calculated) + float(actime_time)

            results[car_number].update(
                {
                    'actime_time': actime_time,
                }
            )

        return results
    except Exception as error:
        raise error


def calculate_avarage_speed(file_name, results):
    try:

        header = []
        for row in read_csv(file_name=file_name):

            if not header:
                header = build_values(row, header)
                continue

            values_dict = build_values(row, header)

            # initiates an entry per car
            if not results.get(values_dict.get('car_number')):
                results[values_dict.get('car_number')] = {}

            car_number = values_dict.get('car_number')
            speed_km_h = values_dict.get('speed_km_h')

            calculated = results.get(car_number, {}).get('speed_km_h_sun', 0)
            sampling = results.get(car_number, {}).pop('sampling', 0) + 1
            speed_sum = float(calculated) + float(speed_km_h)
            results[car_number].update(
                {
                    'speed_km_h_sun': speed_sum,
                    'sampling': sampling,
                    'speed_km_h_avg': speed_sum / sampling
                }
            )
        return results

    except Exception as error:
        raise error


def calculate_number_trips(file_name, results):
    try:
        header = []
        for row in read_csv(file_name=file_name):

            if not header:
                header = build_values(row, header)
                continue

            values_dict = build_values(row, header)

            # initiates an entry per car
            if not results.get(values_dict.get('car_number')):
                results[values_dict.get('car_number')] = {}

            car_number = values_dict.get('car_number')

            calculated = results.get(car_number, {}).get('num_trips', 0)

            results[car_number].update(
                {
                    'num_trips': calculated + 1
                }
            )
        return results

    except Exception as error:
        raise error


def manipulate_data(file_name):
    # with open(os.path.join(FILE_DIR, 'data', filename), 'r') as csv_file:
    results = {}

    try:
        try:
            list_threads = [
                threading.Thread(
                    target=calculate_distance,
                    args=(
                        file_name,
                        results
                    )
                ),

                threading.Thread(
                    target=calculate_total_operating_time,
                    args=(
                        file_name,
                        results
                    )
                ),

                threading.Thread(
                    target=calculate_utilization,
                    args=(
                        file_name,
                        results
                    )
                ),

                threading.Thread(
                    target=calculate_avarage_speed,
                    args=(
                        file_name,
                        results
                    )
                ),

                threading.Thread(
                    target=calculate_number_trips,
                    args=(
                        file_name,
                        results
                    )
                )
            ]
            [t.start() for t in list_threads]
            [t.join() for t in list_threads]

        except Exception as error:
            create_output_file(
                error=True,
                msg='Error : {0}'.format(
                    error.message if hasattr(error, 'message') else error
                )
            )

        create_output_file(results=results)

    except Exception as error:
        create_output_file(
            error=True, msg='Error row {0}: {1}'.format(
                0,
                error.message if hasattr(error, 'message') else error
            )
        )


def create_output_file(error=False, msg='', results={}):
    """
        Function to create a output log
        :param error:
        :param msg:
        :param results:
        :return: None
    """

    if not os.path.exists(os.path.join(FILE_DIR, 'data', 'output.csv')):
        new_file = open(os.path.join(FILE_DIR, 'data', 'output.csv'), mode='w+')
        new_file.close()

    with open(os.path.join(FILE_DIR, 'data', 'output.csv'), mode='r+') as output:

        csv_reader = csv.reader(output, delimiter=',')
        output_writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if not list(csv_reader):
            # write header
            output_writer.writerow(
                ['car_num', 'total_distance', 'operating_time', 'utilisation', 'avg_speed', 'num_trips', 'ERROR', 'MSG']
            )

        if not error:
            for key, value in results.items():
                output_writer.writerow(
                    [
                        key,
                        results[key].get('distance'),
                        results[key].get('duration_h'),
                        results[key].get('distance'),
                        results[key].get('speed_km_h_avg'),
                        results[key].get('num_trips'),
                        error,
                        msg
                    ]
                )
        else:
            output_writer.writerow(
                ['', '', '', '', '', '', error, msg]
            )


if __name__ == '__main__':
    start = time.perf_counter()
    manipulate_data('da_challenge_data_sanitized.csv')
    finish = time.perf_counter()
    print(f"Processed in {finish - start:0.4f} seconds")
