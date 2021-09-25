import csv
import os
import datetime
import time

FILE_DIR = os.path.dirname(os.path.realpath(__file__))


def read_csv(file_name):
    """
    Creates a generator to improve memory usage
    :param file_name:
    :return:
    """
    for row in open(os.path.join(FILE_DIR, 'data', file_name), "r"):
        yield row


def calculate_distance(car_number, distance, results):
    try:

        calculated = results.get(car_number, {}).get('distance', 0)
        results[car_number].update(
            {
                'distance': float(calculated) + float(distance)
            }
        )

    except Exception as error:
        raise error


def calculate_total_operating_time(car_number, duration_s, results):
    try:
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

    except Exception as error:
        raise error


def calculate_utilization(car_number, actime_time, results):
    calculated = results.get(car_number, {}).get('actime_time', 0)
    actime_time = float(calculated) + float(actime_time)

    results[car_number].update(
        {
            'actime_time': actime_time,
        }
    )


def calculate_avarage_speed(car_number, speed_km_h, results):
    try:

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

    except Exception as error:
        raise error


def calculate_number_trips(car_number, results):
    try:

        calculated = results.get(car_number, {}).get('num_trips', 0)

        results[car_number].update(
            {
                'num_trips': calculated + 1
            }
        )

    except Exception as error:
        raise error


def manipulate_data(file_name):
    # with open(os.path.join(FILE_DIR, 'data', filename), 'r') as csv_file:
    header = []
    results = {}

    try:
        count = 0
        for row in read_csv(file_name=file_name):
            try:
                count += 1
                values = row.rstrip('\n').split(',')

                if len(values) < 18:
                    raise Exception('There are missing values')

                if not header:
                    header = values
                    continue

                values_dict = {header[index]: values[index] for index in range(0, len(header))}

                print(values_dict)

                # initiates an entry per car
                if not results.get(values_dict.get('car_number')):
                    results[values_dict.get('car_number')] = {}

                calculate_distance(
                    values_dict.get('car_number'),
                    values_dict.get('distance_m'),
                    results
                )
                calculate_total_operating_time(
                    values_dict.get('car_number'),
                    values_dict.get('duration_s'),
                    results
                )
                calculate_utilization(
                    values_dict.get('car_number'),
                    values_dict.get('ignition_on'),
                    results
                )
                calculate_avarage_speed(
                    values_dict.get('car_number'),
                    values_dict.get('speed_km_h'),
                    results
                )
                calculate_number_trips(
                    values_dict.get('car_number'),
                    results
                )
            except Exception as error:
                create_output_file(
                    error=True,
                    msg='Error row {0}: {1}'.format(
                        count,
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


