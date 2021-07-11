from pymongo import DeleteMany
import pandas as pd
from utils.database_settings import DatabaseSettings


class DataProcessor(DatabaseSettings):
    def __init__(self, sunat_file, covid_contract_file):
        super().__init__()
        self.sunat_file = sunat_file
        self.covid_contracts_file = covid_contract_file

    def clean_database(self):
        self.db.companiesList.bulk_write([DeleteMany({})])
        self.db.companiesLocationData.bulk_write([DeleteMany({})])
        self.db.contractData.bulk_write([DeleteMany({})])

    def clean_companies(self):
        self.db.companiesList.bulk_write([DeleteMany({})])

    def clean_contracts(self):
        self.db.companiesLocationData.bulk_write([DeleteMany({})])

    def parse_sunat_file(self):
        with open(self.sunat_file, 'r', errors='replace', encoding='UTF-8') as sunat_file:
            next(sunat_file)
            lines_read = 0
            companies = []
            print(self.db.name)
            for company in sunat_file:
                if company[0] != "1":
                    lines_read += 1
                    company_data_array = company.split('|')
                    address = f'{company_data_array[5]} {company_data_array[6]} {company_data_array[9]}'
                    companies.append({
                        'ruc': company_data_array[0],
                        'razon_social': company_data_array[1],
                        'estado_contribuyente': company_data_array[2],
                        'condicion_domicilio': company_data_array[3],
                        'ubigeo': company_data_array[4],
                        'direccion': address
                    })
            self.db.companiesList.insert_many(companies)
            print(f'Cantidad de documentos en la base de datos: {self.db.companiesList.count_documents({})}')

    def parse_covid_contract_file(self):
        data = pd.read_excel(self.covid_contracts_file)
        df = pd.DataFrame(data, columns=['CODIGOENTIDAD', 'RUC_ENTIDAD', 'ENTIDAD', 'FECHACONVOCATORIA', 'RUCPROVEEDOR',
                                         'PROVEEDOR', 'RUBROS', 'MONTOREFERENCIALSOLES', 'MONTOADJUDICADOSOLES',
                                         'ITEMCONVOCA_UNIDADMEDIDA'])
        result = []
        for index, row in df.iterrows():
            company_data = self.db.companiesList.find_one({'ruc': row['RUCPROVEEDOR']})
            company_information = {
                'code_entity': row['CODIGOENTIDAD'],
                'ruc_entity': row['RUC_ENTIDAD'],
                'entity': row['ENTIDAD'],
                'competition_date': row['FECHACONVOCATORIA'],
                'ruc_provider': row['RUCPROVEEDOR'],
                'name_provider': row['PROVEEDOR'],
                'heading_provider': row['RUBROS'],
                'referential_price': row['MONTOREFERENCIALSOLES'],
                'official_price': row['MONTOADJUDICADOSOLES'],
                'units': row['ITEMCONVOCA_UNIDADMEDIDA']
            }
            if company_data:
                company_information['ubigeo_provider'] = company_data['ubigeo']
                company_information['address_prodiver'] = company_data['direccion']
                result.append(company_information)
            else:
                company_information['ubigeo_provider'] = '0'
                company_information['address_prodiver'] = '- - -'
                result.append(company_information)
            print(index)

        if len(result) > 0:
            self.db.contractData.insert_many(result)
            print(self.db.contractData.count_documents({}))

    def parse_head_provider_data(self):
        result = []
        cursor = self.db.fullContractData
        for row in cursor.find():
            result.append({
                'code_entity': row['code_entity'],
                'ruc_entity': row['ruc_entity'],
                'entity': row['entity'],
                'competition_date': row['competition_date'],
                'ruc_provider': row['ruc_provider'],
                'name_provider': row['name_provider'],
                'ubigeo_provider': row['ubigeo_provider'],
                'address_prodiver': row['address_prodiver'],
                'heading_provider': row['heading_provider'],
                'referential_money': row['referential_price'],
                'contract_money': row['official_price'],
                'units': row['units'],
            })

        if len(result) > 0:
            self.db.headProviderData.insert_many(result)
            print(f'Se inserto: {len(result)} registros')

    def get_valid_header_providers(self):
        cursor = self.db.fullContractData
        result = []
        for document in cursor.find():
            entity = self.db.headProviderData.find_one({'ruc_entity': document['ruc_entity']})
            if entity is not None:
                new_entity = {
                    'code_entity': entity['code_entity'],
                    'ruc_entity': entity['ruc_entity'],
                    'entity': entity['entity'],
                    'competition_date': entity['competition_date'],
                    'ruc_provider': entity['ruc_provider'],
                    'name_provider': entity['name_provider'],
                    'heading_provider': entity['heading_provider'],
                    'referential_money': entity['referential_money'],
                    'contract_money': entity['contract_money'],
                    'quantity': entity['units']
                }
                result.append(new_entity)

        if len(result) > 0:
            print(f'Se insertaron {len(result)} proveedores validos')
            self.db.validHeadProviderData.insert_many(result)
        else:
            print('Ningun registro insertado')

    @staticmethod
    def convert_in_csv(fields, cursor, file_name, sum_array=False, array_field=None, sub_field=None):
        res = []
        for document in cursor.find():
            row = {}
            for name, field in fields.items():
                row[name] = document[field]
            if sum_array and array_field is not None:
                sum_of_elements = 0
                for element in document[array_field]:
                    if sub_field is not None:
                        sum_of_elements = sum_of_elements + float(element[sub_field])
                    else:
                        sum_of_elements = sum_of_elements + element
                row['sum_' + str(array_field)] = sum_of_elements
            res.append(row)
        data_frame = pd.DataFrame(res)
        data_frame.to_csv(r'' + str(file_name) + '.csv', index=False, header=True)
