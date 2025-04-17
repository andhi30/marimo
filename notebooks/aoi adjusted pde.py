import marimo

__generated_with = "0.12.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import pandas as pd
    from google.cloud import bigquery
    from pathlib import Path
    from jinja2 import Template
    from typing import Optional, Dict, Any

    project_id = "p-gojek-data-analytics-access"

    bq_client = bigquery.Client(project=project_id)

    def query_bq(query: str, bq_client: Optional[bigquery.Client] = None) -> pd.DataFrame:
      """
      Query BigQuery and return results as a DataFrame.

      Args:
        query: SQL query string
        bq_client: BigQuery client (optional, will create one if None)
      Returns:
        DataFrame with query results
      """
      if not bq_client:
        bq_client = bigquery.Client(project=project_id)

      job = bq_client.query(query)
      result = job.result()

      df = result.to_dataframe(create_bqstorage_client=True)

      # Convert datetime columns to pandas datetime
      for col in df.select_dtypes(include=['datetime64[ns, UTC]']).columns:
        df[col] = pd.to_datetime(df[col].dt.tz_localize(None))

      return df


    def load_df_from_sql(
      bq_client: Optional[bigquery.Client] = None, 
      sql_file: Optional[Path] = None, 
      sql_string: Optional[str] = None,
      sql_params: Optional[Dict[str, Any]] = None, 
      verbose: bool = False
    ) -> pd.DataFrame:
      """
      Load DataFrame from SQL file or string with optional templating.

      Args:
        bq_client: BigQuery client (optional, will create one if None)
        sql_file: Path to SQL file (optional if sql_string provided)
        sql_string: Raw SQL string (optional if sql_file provided)
        sql_params: Parameters for SQL template
        verbose: Whether to print the query

      Returns:
        DataFrame with query results

      Raises:
        ValueError: If neither sql_file nor sql_string is provided
      """
      if not bq_client:
        bq_client = bigquery.Client(project=project_id)

      if sql_file and not sql_string:
        with open(sql_file, 'r') as f:
          query = f.read()
      elif sql_string:
        query = sql_string
      else:
        raise ValueError("Either sql_file or sql_string must be provided")

      if sql_params:
        template = Template(query)
        query = template.render(**sql_params)

      if verbose:
        print(query)

      return query_bq(query, bq_client)
    return (
        Any,
        Dict,
        Optional,
        Path,
        Template,
        bigquery,
        bq_client,
        load_df_from_sql,
        pd,
        project_id,
        query_bq,
    )


@app.cell
def _(bq_client, query_bq):
    query = """
    select *
    from `cartography-integration.operation.detail_places_booking_transport`
    where date(booking_time) between current_date()-2 and current_date()-1
        and booking_pickup_latitude != aoi_adjusted_booking_pickup_latitude
        and booking_pickup_longitude != aoi_adjusted_booking_pickup_longitude
        and aoi_adjusted_pickup_distance_error - pickup_distance_error > 50
    """

    df = query_bq(query=query, bq_client=bq_client)

    return df, query


@app.cell
def _(df):
    df.info()
    return


@app.cell
def _(df):
    df.to_feather("notebooks/public/transport_booking.feather")
    return


@app.cell
def _(pd):
    data = pd.read_feather("notebooks/public/transport_booking.feather")
    data.head()
    return (data,)


@app.cell
def _():
    # data.columns
    return


@app.cell
def _(data, pd):
    import folium

    def create_map(df: pd.DataFrame, sample_size: int = 100) -> folium.Map:
        """
        Creates an interactive map with booking and driver locations,
        connecting lines, and distance error tooltips.

        Args:
            df: DataFrame containing booking and driver location data.
            sample_size: Number of data points to sample for the map.

        Returns:
            A folium Map object.
        """

        # Sample the DataFrame
        if len(df) > sample_size:
            _df = df.sample(sample_size, random_state=42)  # for reproducibility
        else:
            _df = df.copy()

        # Calculate the center of the map
        center_lat = _df['booking_pickup_latitude'].mean()
        center_lon = _df['booking_pickup_longitude'].mean()
        # center_lat = _df.iloc[0]['booking_pickup_latitude']
        # center_lon = _df.iloc[0]['booking_pickup_longitude']

        # Create the map
        m = folium.Map(location=[center_lat, center_lon], zoom_start=5)

        # Iterate through the DataFrame and add markers and lines
        for index, row in _df.iterrows():
            # Booking pickup location
            folium.Marker(
                location=[row['booking_pickup_latitude'], row['booking_pickup_longitude']],
                popup=f"Booking: {row['booking_number']}",
                icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(m)

            # Driver pickup location
            folium.Marker(
                location=[row['driver_pickup_latitude'], row['driver_pickup_longitude']],
                popup=f"Driver",
                icon=folium.Icon(color='green', icon='car')
            ).add_to(m)

            # AOI adjusted booking pickup location
            folium.Marker(
                location=[row['aoi_adjusted_booking_pickup_latitude'], row['aoi_adjusted_booking_pickup_longitude']],
                popup=f"AOI Adjusted",
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(m)

            # Line between booking and driver
            folium.PolyLine(
                locations=[(row['booking_pickup_latitude'], row['booking_pickup_longitude']),
                           (row['driver_pickup_latitude'], row['driver_pickup_longitude'])],
                color='blue',
                weight=2,
                tooltip=f"Pickup Distance Error: {row['pickup_distance_error']:.2f}"
            ).add_to(m)

            # Line between AOI adjusted and driver
            folium.PolyLine(
                locations=[(row['aoi_adjusted_booking_pickup_latitude'], row['aoi_adjusted_booking_pickup_longitude']),
                           (row['driver_pickup_latitude'], row['driver_pickup_longitude'])],
                color='red',
                weight=2,
                tooltip=f"AOI Adjusted Distance Error: {row['aoi_adjusted_pickup_distance_error']:.2f}"
            ).add_to(m)

        return m

    m = create_map(data, sample_size=10)
    m
    return create_map, folium, m


if __name__ == "__main__":
    app.run()
