"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_date: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """
    import zipfile
    import pandas as pd
    import glob
    import os
    
    def leer_archivos(ruta_input):
        df_lista = []
        for file in glob.glob(f"{ruta_input}/*"):
            # Abrir el zip y leer el csv que contiene
            with zipfile.ZipFile(file, 'r') as zip_ref:
                nombre_csv = zip_ref.namelist()[0] 
                with zip_ref.open(nombre_csv) as archivo_csv:
                    df = pd.read_csv(archivo_csv)
            df_lista.append(df)

        df_unido = pd.concat(df_lista, ignore_index=True)
        return df_unido
    
    def limpiar_datos(df):
        df['job'] = df['job'].str.replace('.', '', regex=False)  
        df['job'] = df['job'].str.replace('-', '_', regex=False) 
        df['education'] = df['education'].str.replace('.', '_', regex=False)  
        df['education'] = df['education'].replace('unknown', pd.NA)
        df['credit_default'] = df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
        df['mortgage'] = df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
        df['previous_outcome'] = df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
        df['campaign_outcome'] = df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
        df['month'] = df['month'].str.capitalize()  # Asegurar que los valores están en formato adecuado 
        df['last_contact_date'] = pd.to_datetime( df['day'].astype(str) + ' ' + df['month'] + ' 2022', format='%d %b %Y')
        df['last_contact_date'] = df['last_contact_date'].dt.strftime('%Y-%m-%d')
        return df

    
    def guardar_datos(output_ruta, df, nombre):
        if not os.path.exists(output_ruta):
            os.makedirs(output_ruta)
        df.to_csv(f'{output_ruta}/{nombre}.csv', encoding='utf-8', index=False)
    
    input_ruta = 'files/input'
    output_ruta = 'files/output'
    df = leer_archivos(input_ruta)
    df_final = limpiar_datos(df)

    df_client = df_final[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    df_campaign = df_final[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "last_contact_date"]]
    df_economics = df_final[["client_id", "cons_price_idx", "euribor_three_months"]]

    guardar_datos(output_ruta, df_client, "client")
    guardar_datos(output_ruta, df_campaign, "campaign")
    guardar_datos(output_ruta, df_economics, "economics")



if __name__ == "__main__":
    clean_campaign_data()
