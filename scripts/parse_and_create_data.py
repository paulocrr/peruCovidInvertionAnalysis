from src.data_proccessor import DataProcessor
from src.geographic_information_parser import GeographicInformationParser
from src.sunat_crawler import SunatCrawler


if __name__ == '__main__':

    print('Creado la estructura en la base de datos y obteniendo la data')

    data_processor = DataProcessor(sunat_file='data/padron_reducido_ruc.txt', covid_contract_file='data/contratos.xlsx')
    geographic_parser = GeographicInformationParser()
    sunat_crawler = SunatCrawler()

    data_processor.parse_sunat_file()
    data_processor.parse_covid_contract_file()

    geographic_parser.get_location_from_provider()
    geographic_parser.get_location_from_entities()
    data_processor.parse_head_provider_data()
    data_processor.get_valid_header_providers()

    sunat_crawler.get_all_legal_rep()
    sunat_crawler.get_entity_provider_data()
    geographic_parser.get_entity_and_provider_department()
    geographic_parser.copy_all_contracts_with_department()
