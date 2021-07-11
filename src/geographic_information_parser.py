from geopy.geocoders import Nominatim
from utils.database_settings import DatabaseSettings


class GeographicInformationParser(DatabaseSettings):
    def __init__(self):
        super().__init__()
        self.geolocator = Nominatim(user_agent="http")

    def get_location_from_provider(self):
        cursor = self.db.contractData
        result = []
        exclude_companies = []
        document_count = 0
        for document in cursor.find():
            document_count += 1
            print(document_count)
            if document['address_prodiver'] != '- - -' and document['ubigeo_provider'] != '0':
                location = self.geolocator.geocode(document['address_prodiver'], timeout=None, country_codes=['pe'])
                print(location)
                company_information = {
                    'code_entity': document['code_entity'],
                    'ruc_entity': document['ruc_entity'],
                    'entity': document['entity'],
                    'competition_date': document['competition_date'],
                    'ruc_provider': document['ruc_provider'],
                    'name_provider': document['name_provider'],
                    'ubigeo_provider': document['ubigeo_provider'],
                    'address_prodiver': document['address_prodiver'],
                    'heading_provider': document['heading_provider'],
                    'referential_price': document['referential_price'],
                    'official_price': document['official_price'],
                    'units': document['units']
                }
                if location is not None:
                    company_information['lat'] = location.latitude
                    company_information['lng'] = location.longitude
                    result.append(company_information)
                else:
                    company_information['lat'] = 0
                    company_information['lng'] = 0
                    exclude_companies.append(company_information)

        if len(result) > 0:
            self.db.companiesLocationData.insert_many(result)
            print(f'Cantidad de compañias con ubicación: {self.db.companiesLocationData.count_documents({})}')

        if len(exclude_companies) > 0:
            self.db.companiesWithNoLocation.insert_many(exclude_companies)
            print(f'Cantidad de compañias excluidas: {self.db.companiesWithNoLocation.count_documents({})}')

    def get_location_from_entities(self):
        cursor = self.db.companiesLocationData
        result = []
        exclude_entities = []
        document_count = 0
        for document in cursor.find():
            document_count += 1
            entity_data = self.db.companiesList.find_one({'ruc': str(document['ruc_entity'])})
            if entity_data is not None and entity_data['direccion'] != '- - -':
                location = self.geolocator.geocode(entity_data['direccion'], timeout=None, country_codes=['pe'])
                print(document_count)
                company_data = {
                    'code_entity': document['code_entity'],
                    'ruc_entity': document['ruc_entity'],
                    'entity': document['entity'],
                    'competition_date': document['competition_date'],
                    'ruc_provider': document['ruc_provider'],
                    'name_provider': document['name_provider'],
                    'ubigeo_provider': document['ubigeo_provider'],
                    'address_prodiver': document['address_prodiver'],
                    'heading_provider': document['heading_provider'],
                    'referential_price': document['referential_price'],
                    'official_price': document['official_price'],
                    'units': document['units'],
                    'provider_lat': document['lat'],
                    'provider_lng': document['lng']
                }
                if location is not None:
                    company_data['entity_lat'] = location.latitude
                    company_data['entity_lng'] = location.longitude
                    result.append(company_data)
                else:
                    company_data['entity_lat'] = 0
                    company_data['entity_lng'] = 0
                    exclude_entities.append(company_data)

        if len(result) > 0:
            self.db.fullContractData.insert_many(result)
            print(self.db.fullContractData.count_documents({}))

        if len(exclude_entities) > 0:
            self.db.noLocationEntity.insert_many(exclude_entities)
            print(self.db.noLocationEntity.count_documents({}))

    def get_department_by_coordinates(self, coordinates):
        place_information = self.geolocator.reverse(coordinates)
        # print(place_information.raw['address'])
        if 'state' in place_information.raw['address'].keys():
            department = place_information.raw['address']['state']
        else:
            department = place_information.raw['address']['region']
        department = department.replace("Departamento de ", "").upper()
        # print(department)
        return department

    def get_entity_and_provider_department(self):
        cursor = self.db.validContractData
        res = []
        for document in cursor.find():
            temp = dict(document)
            temp['entity_department'] = self.get_department_by_coordinates(f'{document["entity_lat"]} ,'
                                                                           f'{document["entity_lng"]}')
            temp['provider_department'] = self.get_department_by_coordinates(f'{document["provider_lat"]} ,'
                                                                             f'{document["provider_lng"]}')
            temp.pop('_id')
            print(temp)
            res.append(temp)

        self.db.validContractData.insert_many(res)
        print(res)

    def copy_all_contracts_with_department(self):
        cursor = self.db.validContractData
        res = []

        for document in cursor.find({"entity_department": {"$exists": True}}):
            temp = dict(document)
            temp.pop('_id')
            res.append(temp)
        self.db.validContractDataWithDepartment.insert_many(res)
