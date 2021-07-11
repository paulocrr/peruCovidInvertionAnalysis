from src.vizualizator import Visualizator


if __name__ == '__main__':

    print('Creado todas las visualizaciones')

    visualizator = Visualizator()

    visualizator.get_number_of_contract_per_company()
    visualizator.entities_with_more_contracts()
    visualizator.get_map_with_number_of_companies()
    visualizator.get_map_with_number_of_entities()

    visualizator.header_provider_histogram()

    visualizator.get_contract_chronopleth()
    visualizator.get_entities_provider_arc_map()
    visualizator.entities_provider_with_debt_arc_map()
