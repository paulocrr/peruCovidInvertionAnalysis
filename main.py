
if __name__ == '__main__':

    # Crea la estructura necesaria en la base de datos para crear las visualizaciones
    exec(open('scripts/parse_and_create_data.py').read())

    # Crea todas las visualizaciones
    exec(open('scripts/generate_all_visualizations.py').read())
