from utils.database_settings import DatabaseSettings
import geopandas as gpd
import pandas as pd
import folium
import branca
import requests
import pydeck as pdk
from unidecode import unidecode
from folium.features import GeoJsonTooltip
from src.data_proccessor import DataProcessor
import plotly.io as pio
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from folium import plugins


class Visualizator(DatabaseSettings):
    def __init__(self):
        super().__init__()

    def get_contract_chronopleth(self):
        cursor = self.db.validContractDataWithDepartment
        response = requests.get(
            r"https://raw.githubusercontent.com/juaneladio/peru-geojson/master/peru_departamental_simple.geojson")
        data = response.json()
        states = gpd.GeoDataFrame.from_features(data, crs='EPSG:4326')
        histogram = {}
        for row in states.iterrows():
            print(row[1]['NOMBDEP'])
            histogram[row[1]['NOMBDEP']] = 0

        for document in cursor.find():
            histogram[unidecode(document['entity_department'])] = histogram[unidecode(document['entity_department'])] \
                                                                  + 1

        res = []

        for key, element in histogram.items():
            res.append({
                'NOMBDEP': key,
                'CONTACT_NUMBER': element
            })

        histogram_df = pd.DataFrame(res)

        pd.options.display.max_columns = None
        pd.options.display.max_rows = None
        print(states.head())
        print(histogram_df.head())
        state_merges = states.merge(histogram_df, how='left', left_on='NOMBDEP', right_on='NOMBDEP')
        print(state_merges.columns)
        colormap = branca.colormap.LinearColormap(
            vmin=state_merges['CONTACT_NUMBER'].quantile(0.0),
            vmax=state_merges['CONTACT_NUMBER'].quantile(1),
            colors=['red', 'orange', 'white', 'green', 'darkgreen'],
            caption="Cantidad de contratos requeridos por departamento",
        )

        m = folium.Map(location=[-12.0630149, -77.0296179], zoom_start=6)

        tooltip = GeoJsonTooltip(
            fields=['NOMBDEP', 'CONTACT_NUMBER'],
            aliases=['State: ', "Cantidad de contratos:"],
            localize=True,
            sticky=False,
            labels=True,
            style="""
                background-color: #F0EFEF;
                border: 2px solid black;
                border-radius: 3px;
                box-shadow: 3px;
            """,
            max_width=800,
        )

        colormap.add_to(m)

        folium.GeoJson(
            state_merges,
            style_function=lambda x: {
                "fillColor": colormap(x["properties"]["CONTACT_NUMBER"])
                if x["properties"]["CONTACT_NUMBER"] is not None
                else "transparent",
                "color": "black",
                "fillOpacity": 0.4,
            },
            tooltip=tooltip,
        ).add_to(m)

        m.save("number_of_contracts_per_departemnt.html")

    def get_entities_provider_arc_map(self):
        cursor = self.db.validContractDataWithDepartment
        res = []
        for document in cursor.find():
            row = {
                'entity': document['entity'],
                'entity_lat': document['entity_lat'],
                'entity_lng': document['entity_lng'],
                'provider_name': document['provider_name'],
                'provider_lat': document['provider_lat'],
                'provider_lng': document['provider_lng'],
                'contract_money': document['contract_money'],
                'entity_department': document['entity_department'],
                'provider_department': document['provider_department'],
                'have_coactive_debt': len(document['coactive_debt']) > 0
            }
            res.append(row)
        Visualizator.make_arc_map("entity_provider_arc_map", res)

    def entities_provider_with_debt_arc_map(self):
        cursor = self.db.validContractDataWithDepartment
        res = []
        for document in cursor.find():
            if len(document['coactive_debt']) > 0:
                row = {
                    'entity': document['entity'],
                    'entity_lat': document['entity_lat'],
                    'entity_lng': document['entity_lng'],
                    'provider_name': document['provider_name'],
                    'provider_lat': document['provider_lat'],
                    'provider_lng': document['provider_lng'],
                    'contract_money': document['contract_money'],
                    'entity_department': document['entity_department'],
                    'provider_department': document['provider_department']
                }

                sum_of_debt = 0
                for d in document['coactive_debt']:
                    sum_of_debt = sum_of_debt + float(d['debt_amount'])

                row['sum_debt'] = sum_of_debt
                res.append(row)
        Visualizator.make_arc_map("entity_provider_debt_arc_layer", res)

    def generate_coactive_csv(self):
        cursor = self.db.providerDocument
        fields = {
            'provider_ruc': 'provider_ruc',
            'provider_name': 'provider_name',
            'provider_lat': 'provider_lat',
            'provider_lng': 'provider_lng',
            'provider_heading': 'provider_heading'
        }
        DataProcessor.convert_in_csv(file_name='coactive_debt_per_provider', fields=fields, sub_field='debt_amount',
                                     sum_array=True, array_field='coactive_debt', cursor=cursor)

    def get_number_of_contract_per_company(self):
        cursor = self.db.fullContractData
        histogram = {}
        x_axis = []
        y_axis = []
        for document in cursor.find():
            if document['ruc_provider'] not in histogram:
                histogram[document['ruc_provider']] = {'number': 1, 'name': document['name_provider']}
            else:
                histogram[document['ruc_provider']]['number'] += 1

        histogram = self.get_top_n_of_histogram(histogram, 11)
        print(histogram)
        for key, value in histogram.items():
            x_axis.append(value['name'][0:10])
            y_axis.append(value['number'])

        pio.renderers.default = 'png'
        fig = go.Figure([go.Bar(x=x_axis, y=y_axis)])
        fig.write_image('number_of_contract_per_company.png')

    def entities_with_more_contracts(self):
        cursor = self.db.fullContractData
        histogram = {}
        x_axis = []
        y_axis = []
        for document in cursor.find():
            ruc_entity = str(document['ruc_entity'])
            if ruc_entity not in histogram:
                histogram[ruc_entity] = {'number': 1, 'name': document['entity']}
            else:
                histogram[ruc_entity]['number'] += 1

        histogram = self.get_top_n_of_histogram(histogram, 11)
        for key, value in histogram.items():
            x_axis.append(value['name'][0:5])
            y_axis.append(value['number'])

        x_pos = [i for i, _ in enumerate(x_axis)]
        plt.bar(x_pos, y_axis)
        plt.xlabel("Compañia")
        plt.ylabel("Cantidad de contratos")
        plt.title("Cantidad de contratos realizados por compañia")
        plt.xticks(x_pos, x_axis)
        plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
        plt.savefig('entities_with_more_contracts.png')
        plt.show()

    def get_map_with_number_of_companies(self):
        m = folium.Map(location=[-12.0266034, -77.1278612], zoom_start=6.4)
        cursor = self.db.fullContractData
        companies_lat = []
        companies_lng = []
        for document in cursor.find():
            companies_lat.append(document['provider_lat'])
            companies_lng.append(document['provider_lng'])

        plugins.FastMarkerCluster(data=list(zip(companies_lat, companies_lng))).add_to(m)
        m.save("number_of_companies_by_location.html")

    def get_map_with_number_of_entities(self):
        m = folium.Map(location=[-12.0266034, -77.1278612], zoom_start=6.4)
        cursor = self.db.fullContractData
        entities_lat = []
        entities_lng = []
        hash_map = {}
        for document in cursor.find():
            if document['ruc_entity'] not in hash_map:
                entities_lat.append(document['entity_lat'])
                entities_lng.append(document['entity_lng'])
                hash_map[document['ruc_entity']] = 1

        plugins.FastMarkerCluster(data=list(zip(entities_lat, entities_lng))).add_to(m)
        m.save("number_of_entities_by_location.html")

    def header_provider_histogram(self):
        cursor = self.db.validHeadProviderData
        histogram = {}
        x_axis = []
        y_axis = []
        for document in cursor.find():
            heading_provider_entity = str(document['heading_provider'])
            if heading_provider_entity not in histogram:
                histogram[heading_provider_entity] = {'number': document['contract_money'], 'name': document['entity']}
            else:
                histogram[heading_provider_entity]['number'] += document['contract_money']

        histogram = Visualizator.get_top_n_of_histogram(histogram, 11)
        for key, value in histogram.items():
            x_axis.append(key[0:5])
            y_axis.append(value['number'])

        x_pos = [i for i, _ in enumerate(x_axis)]
        plt.bar(x_pos, y_axis)
        plt.xlabel("Rubro")
        plt.ylabel("Monto")
        plt.title("Top 11 de rubos con mas inversión")
        plt.xticks(x_pos, x_axis)
        plt.setp(plt.gca().get_xticklabels(), rotation=30, horizontalalignment='right')
        plt.savefig('top_11_of_headers.png')
        plt.show()

    @staticmethod
    def get_top_n_of_histogram(histogram, n):
        histogram = dict(sorted(histogram.items(), key=lambda item: item[1]['number'], reverse=True))
        histogram = {k: histogram[k] for k in list(histogram)[:n]}
        print(histogram)
        return histogram

    @staticmethod
    def make_arc_map(file_name, data):
        data_frame = pd.DataFrame(data)
        green_color = [0, 255, 0, 40]
        red_color = [240, 100, 0, 40]
        arc_layer = pdk.Layer(
            "ArcLayer",
            data=data_frame,
            get_width="S000 * 2",
            get_source_position=["entity_lng", "entity_lat"],
            get_target_position=["provider_lng", "provider_lat"],
            get_tilt=15,
            get_source_color=red_color,
            get_target_color=green_color,
            pickable=True,
            auto_highlight=True,
        )

        view_state = pdk.ViewState(latitude=-12.0630149, longitude=-77.0296179, bearing=45, pitch=50, zoom=8)
        tooltip_text = {"html": "Entidad: {entity}<br />Origen: {entity_department} <br />Proveedor: {provider_name} "
                                "<br /> Tiene deuda Coactiva: {have_coactive_debt}<br />Destino: {provider_department} "
                                "<br /> Monto del Contrato: S/ {contract_money} "
                                "<br /> La entidad esta en color rojo y el proveedor en color verde"}

        r = pdk.Deck(arc_layer, initial_view_state=view_state, tooltip=tooltip_text)
        r.to_html(str(file_name)+".html", notebook_display=False)
