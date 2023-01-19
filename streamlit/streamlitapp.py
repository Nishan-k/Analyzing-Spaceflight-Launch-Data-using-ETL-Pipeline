import streamlit as st
import psycopg2


# TODO:1 -> Setup the connection to postgres:
conn = psycopg2.connect(
host="postgres",
database="rocket_info",
user="airflow",
password="airflow"
)
cursor = conn.cursor()




# TODO:2 -> Get all the records from the table:
def get_all_records(cursor):
    """This function returns all the record currently in the table."""

    sql = """SELECT * FROM rocket_info_table; """
    cursor.execute(sql)
    data = cursor.fetchall()

    return data

data = get_all_records(cursor)



# TODO:3 -> Create a drop down, from where user can select the rocket names:
rocket_names = []

for i in range(0, len(data)):
    rocket_names.append(data[i][1])


st.subheader("Please Select The Name Of The Rocket:")
image_name = st.selectbox(
    'This is not needed',
    (rocket_names),
    label_visibility="hidden"
    
)

if image_name:
    sql = f"""
        SELECT name, image_url, launch_status, window_start, window_end,
         launcher_name, launcher_type,total_launch_count, total_landing_count
          FROM rocket_info_table WHERE name = '{image_name}'
    """
    cursor.execute(sql)
    data = cursor.fetchall()
name, image_url, launch_status, window_start, window_end, launcher_name, launcher_type, total_launch_count, total_landing_count = data[0]

st.image(image_url)
st.header(f":blue[{name}]")
st.subheader("Descriptions:")
st.markdown(f"**LAUNCH STATUS**: {launch_status}")
st.markdown(f"**WINDOW START**: {window_start}")
st.markdown(f"**WINDOW END**: {window_end}")
st.markdown(f"**LAUNCHER NAME**: {launcher_name}")
st.markdown(f"**LAUNCHER TYPE**: {launcher_type}")
st.markdown(f"**TOTAL LAUNCHES**: {total_launch_count}")
st.markdown(f"**TOTAL LANDINGS**: {total_landing_count}")