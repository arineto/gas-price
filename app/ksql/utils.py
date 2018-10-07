import json
import requests
from csv import DictReader, DictWriter


def get_location(address):
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    payload = {
        'address': address,
        'key': 'AIzaSyAd_X9ruW9HoIu8xw-jsf0FgP3p8r-rUHs'
    }
    print(f'making location request for: {address}')
    response = requests.get(url, params=payload)

    data = json.loads(response.content)
    location = data['results'][0]['geometry']['location']
    print(f'api returned: {location}')
    return location


def read_data():
    data = []
    with open('sample_data/gas_stations.csv', 'r') as csv_file:
        reader = DictReader(csv_file, delimiter=';')
        data = [row for row in reader]
    return data


def wrire_data(data):
    with open('sample_data/gas_stations2.csv', 'w') as csvfile:
        fieldnames = data[0].keys()
        writer = DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in data:
            writer.writerow(row)


def run():
    data = read_data()
    new_data = []
    for row in data:
        address = f'{row["endereco"]}, {row["bairro"]}, Recife'
        location = get_location(address)
        row.update({
            'latitude': location['lat'],
            'longitude': location['lng'],
        })
        new_data.append(row)
    wrire_data(new_data)
