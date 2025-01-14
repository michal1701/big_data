from pprint import pprint

import requests

"""
example response
{'geometry': {'coordinates': [-104.421101, 31.68425514, 6.131567383],
              'type': 'Point'},
 'id': 'tx2023abqj',
 'properties': {'alert': None,
                'cdi': 2.7,
                'code': '2023abqj',
                'detail': 'https://earthquake.usgs.gov/fdsnws/event/1/query?eventid=tx2023abqj&format=geojson',
                'dmin': 0.005694830054,
                'felt': 3,
                'gap': 58,
                'ids': ',tx2023abqi,us7000j1h5,tx2023abqj,',
                'mag': 3.3,
                'magType': 'ml',
                'mmi': None,
                'net': 'tx',
                'nst': 35,
                'place': '54 km S of Whites City, New Mexico',
                'rms': 0.2,
                'sig': 168,
                'sources': ',tx,us,tx,',
                'status': 'reviewed',
                'time': 1672608227177,
                'title': 'M 3.3 - 54 km S of Whites City, New Mexico',
                'tsunami': 0,
                'type': 'earthquake',
                'types': ',dyfi,origin,phase-data,',
                'tz': None,
                'updated': 1678575090040,
                'url': 'https://earthquake.usgs.gov/earthquakes/eventpage/tx2023abqj'},
 'type': 'Feature'}
"""


def main():
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2023-01-01&endtime=2023-01-02&minlatitude=24.396308&maxlatitude=49.384358&minlongitude=-125.0&maxlongitude=-66.93457&minmagnitude=2.5&orderby=time'

    r = requests.get(url)
    data_dict = r.json()
    pprint(data_dict)


if __name__ == '__main__':
    main()
