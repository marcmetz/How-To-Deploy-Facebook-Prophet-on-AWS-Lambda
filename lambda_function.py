"""
######## IMPORTANT ########
Make sure to edit this file in jupyter to keep consistency.

Search for development and uncomment for better insights.
"""
import io
import uuid

import boto3

import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import s3fs
from fbprophet import Prophet

pd.plotting.register_matplotlib_converters()  # https://github.com/facebook/prophet/issues/999

development = False
BUCKET_NAME = "fbprophet"
FORECAST_IMG_BUCKET_NAME = "forecast-images"
ORDER_DATA = "order_data.csv"
EVENT_DATA = "event_data.csv"
THRESHOLD_ORDERS = 50  # Minimum amount of orders needed
MAX_CAP = 1  # Maximum capacity


def parser(file_name: str, *, date_field: str, **kwargs) -> pd.DataFrame:
    """Read CSV data from file and prepare it for further processing."""
    data = pd.read_csv(
        f"s3://{BUCKET_NAME}/{file_name}", parse_dates=[date_field], **kwargs
    )

    # Remove timezone to avoid ValueError in Prophet
    data[date_field] = data[date_field].dt.tz_convert(None)

    return data


# Parse order data
order_data = parser(
    ORDER_DATA, date_field="created", usecols=["created", "total_gross", "event_id"]
)
order_data = order_data.rename(columns={"created": "ds"})

# Parse event data
event_data = parser(EVENT_DATA, date_field="start_date")

if development:
    display(event_data.head(5))
    display(order_data.head(5))


class Forecast:
    def __init__(self, event_id, event_orders):
        self.event_id = event_id
        self.event_orders = event_orders
        self.event_name = event_data.loc[
            event_data["event_id"] == self.event_id, "name"
        ].values[0]
        self.img_key = str(uuid.uuid4())

    def get_percentage_total_gross_cumsum_max_total_gross(self):
        """% of total_gross_cumsum/max_total_gross."""
        max_total_gross = event_data.loc[
            event_data["event_id"] == self.event_id, "max_total_gross"
        ].values[0]
        self.event_orders["total_gross_cumsum"] = self.event_orders[
            "total_gross"
        ].cumsum()

        return self.event_orders["total_gross_cumsum"] / max_total_gross

    def days_to_event_since_last_order(self):
        """Period for forecast."""
        event_start_date = event_data["start_date"].loc[
            event_data["event_id"] == self.event_id
        ]
        last_order_date = self.event_orders["ds"].tail(1)  # Get the last row

        return (
            event_start_date.iat[0] - last_order_date.iat[0]
        ).days  # We must use iat, as these two variables have a different index

    def upload_figures_to_s3(self, m, forecast):
        image_name = f"{self.img_key}.png"
        forecast_img = io.BytesIO()

        # Plot figure
        m.plot(forecast, xlabel=self.event_name, ylabel="% Sold").savefig(
            forecast_img, format="png"
        )
        forecast_img.seek(0)

        # Upload to S3
        s3 = boto3.client("s3")
        s3.upload_fileobj(
            forecast_img,
            FORECAST_IMG_BUCKET_NAME,
            image_name,
            ExtraArgs={"ACL": "public-read"},
        )

    def get_forecast(self):
        """Make future dataframe."""
        self.event_orders[
            "y"
        ] = self.get_percentage_total_gross_cumsum_max_total_gross()

        # Maximum capacity
        self.event_orders["cap"] = MAX_CAP

        # Fit algorithm
        m = Prophet(growth="linear", yearly_seasonality=False)
        m.fit(self.event_orders)

        # Create future dataframe
        days_to_event = self.days_to_event_since_last_order()
        future = m.make_future_dataframe(periods=days_to_event)

        # Set maximum capacity
        future["cap"] = MAX_CAP

        # Predict the future
        forecast = m.predict(future)

        # Upload figures as .png to S3
        self.upload_figures_to_s3(m, forecast)

        return forecast

    def new_row(self, forecast_data):
        """Create new row and append."""
        forecast_data["event_id"] = self.event_id
        forecast_data["img_key"] = self.img_key
        return forecast_data[
            ["event_id", "ds", "yhat", "yhat_lower", "yhat_upper", "img_key"]
        ].tail(1)


def make_event_forecasts():
    data_for_csv = pd.DataFrame(
        columns=["event_id", "ds", "yhat", "yhat_lower", "yhat_upper", "img_key"]
    )
    list_of_event_ids = order_data["event_id"].unique().tolist()

    if development:
        list_of_event_ids = list_of_event_ids[0:8]

    for event_id in list_of_event_ids:
        # Filter by event_id
        event_orders = order_data[order_data.event_id == event_id].copy()

        # Skip if number of row under threshold
        if len(event_orders) < THRESHOLD_ORDERS:
            continue

        forecast = Forecast(event_id, event_orders)
        forecast_data = forecast.get_forecast()
        new_row = forecast.new_row(forecast_data)
        data_for_csv = data_for_csv.append(new_row, ignore_index=True)

    return data_for_csv


def clean_bucket_with_figures() -> None:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(FORECAST_IMG_BUCKET_NAME)
    bucket.objects.all().delete()


def upload_to_s3(data) -> None:
    """Upload forecast csv file to S3 bucket."""
    s3 = s3fs.S3FileSystem(anon=False)

    with s3.open(f"{BUCKET_NAME}/forecast.csv", "w") as file:
        data.to_csv(file)


def lambda_handler(event, context) -> dict:
    clean_bucket_with_figures()
    data_for_csv = make_event_forecasts()
    upload_to_s3(data_for_csv)
    return {"message": "Forecast uploaded"}
