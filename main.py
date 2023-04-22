import snowflake.connector
import pandas as pd
import streamlit as st
import plotly.express as px


# Define Snowflake connection parameters
SNOWFLAKE_ACCOUNT = st.secrets['account']
SNOWFLAKE_USER = st.secrets['user']
SNOWFLAKE_PASSWORD = st.secrets['password']
SNOWFLAKE_DATABASE = 'investments'
SNOWFLAKE_SCHEMA = "investments_schema"

connection = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    # warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
)

USE_DATABASE_QUERY = '''
USE DATABASE {0}
'''.format(SNOWFLAKE_DATABASE)

# Define SQL queries
USE_SCHEMA_QUERY = '''
USE SCHEMA {0}
'''.format(SNOWFLAKE_SCHEMA)


# Define SQL queries
CREATE_INVESTMENT_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS investments (
    id INTEGER AUTOINCREMENT,
    date DATE,
    instrument VARCHAR,
    buy_price FLOAT,
    current_price FLOAT,
    instrument_type VARCHAR
)
'''

INSERT_INVESTMENT_QUERY = '''
INSERT INTO investments (date, instrument, buy_price, current_price, instrument_type)
VALUES (%s, %s, %s, %s, %s)
'''

SELECT_INVESTMENTS_QUERY = '''
SELECT id, date, instrument, buy_price, current_price, instrument_type
FROM investments
'''

UPDATE_INVESTMENT_QUERY = '''
UPDATE investments
SET date=%s, instrument=%s, buy_price=%s, current_price=%s, instrument_type=%s
WHERE id=%s
'''

DELETE_INVESTMENT_QUERY = '''
DELETE FROM investments
WHERE id=%s
'''

# Create investments table if it doesn't exist
with connection.cursor() as cursor:
    cursor.execute(CREATE_INVESTMENT_TABLE_QUERY)

# Define CRUD functions
def create_investment(date, instrument, buy_price, current_price, instrument_type):
    with connection.cursor() as cursor:
        cursor.execute(INSERT_INVESTMENT_QUERY, (date, instrument, buy_price, current_price, instrument_type))
        connection.commit()
        st.success('Investment added successfully!')

def read_investments():
    with connection.cursor() as cursor:
        cursor.execute(SELECT_INVESTMENTS_QUERY)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['id', 'date', 'instrument', 'buy_price', 'current_price', 'instrument_type'])
        return df

def update_investment(id, date, instrument, buy_price, current_price, instrument_type):
    with connection.cursor() as cursor:
        cursor.execute(UPDATE_INVESTMENT_QUERY, (date, instrument, buy_price, current_price, instrument_type, id))
        connection.commit()
        st.success('Investment updated successfully!')

def delete_investment(id):
    with connection.cursor() as cursor:
        cursor.execute(DELETE_INVESTMENT_QUERY, (id,))
        connection.commit()
        st.success('Investment deleted successfully!')

# Define chart functions
def month_wise_investment_chart(df):
    df['month'] = pd.to_datetime(df['date']).dt.to_period('M')
    chart_data = df.groupby('month').sum().reset_index()
    chart = px.bar(chart_data, x='month', y='buy_price')
    st.plotly_chart(chart)

def highest_profitable_investment_chart(df):
    df['profit'] = df['current_price'] - df['buy_price']
    chart_data = df.nlargest(10, 'profit')
    chart = px.bar(chart_data, x='instrument', y='profit', title='Top 10 Most Profitable Investments')
    st.plotly_chart(chart)


# Define Streamlit app
def app():
    st.title('Investment Dashboard')

    # Define form for creating investment
    with st.form(key='create_investment_form'):
        st.header('Add Investment')
        date = st.date_input('Date', value=None, max_value=None, key=None)
        instrument = st.text_input('Instrument', max_chars=None, value='', key=None)
        buy_price = st.number_input('Buy Price')
        current_price = st.number_input('Current Price')
        instrument_type = st.text_input('Instrument Type', max_chars=None, value='', key=None)
        submit_button = st.form_submit_button(label='Add Investment')

    # Create investment if form is submitted
    if submit_button:
        create_investment(date, instrument, buy_price, current_price, instrument_type)

    # Display investments table
    df = read_investments()
    st.dataframe(df)

   

    # Define form for deleting investment
    with st.form(key='delete_investment_form'):
        st.header('Delete Investment')
        selected_id = st.selectbox('Select investment to delete', df['id'].values)
        submit_button = st.form_submit_button(label='Delete Investment')

    # Delete investment if form is submitted
    if submit_button:
        delete_investment(selected_id)

if __name__ == '__main__':
    app()
