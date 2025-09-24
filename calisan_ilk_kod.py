import datetime as dt
import getpass
import os
from harmony import BBox, Client, Collection, Request
import xarray as xr
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from xarray.plot.utils import label_from_attrs

print("Please provide your Earthdata Login credentials to allow data access")
print("Your credentials will only be passed to Earthdata and will not be exposed in the notebook")
username = input("Username:")
harmony_client = Client(auth=(username, getpass.getpass()))


now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

temporal_range= {
    "start":now - dt.timedelta(hours=1, minutes=15),
    "stop":now
}

request = Request(
    collection=Collection(id="C2930763263-LARC_CLOUD"),
    temporal=temporal_range,
    spatial = BBox(-170, 10, -10, 80) 
   
)

job_id = harmony_client.submit(request)
print(f"jobID = {job_id}")

harmony_client.wait_for_processing(job_id, show_progress=True)

download_dir = os.path.expanduser("~/tempo_data")
os.makedirs(download_dir, exist_ok=True)
results = harmony_client.download_all(job_id, directory=download_dir)
all_results_stored = [f.result() for f in results]
print("✓ Data downloaded successfully!")
print(f"✓ Files saved to: {download_dir}")
print(f"✓ Number of files: {len(all_results_stored)}")

file_path = all_results_stored[0]

print(f"file path: {file_path}")

datatree = xr.open_datatree(file_path)
datatree

data_array = datatree["product/vertical_column_troposphere"]
data_array
underlying_array = data_array.values
print(f"Type of underlying array: {type(underlying_array)}")

quality_flags = datatree["product/main_data_quality_flag"]
quality_flags

good_array = data_array.where(quality_flags == 0).squeeze()
good_array

print(f"Data size in memory: {good_array.nbytes / 1e6:.1f} MB")

data_proj = ccrs.PlateCarree()

roads = cfeature.NaturalEarthFeature(
    category="cultural",
    name="roads",
    scale="10m",  
    facecolor="grey",
)

def make_nice_map(axis):
    axis.set_extent([-170, -10, 10, 80], crs=data_proj)
    axis.add_feature(cfeature.OCEAN, color="lightblue")
    axis.add_feature(cfeature.STATES, color="grey", linewidth=1)
    axis.add_feature(roads, linestyle="-", edgecolor="white", linewidth=0.8)

    grid = axis.gridlines(draw_labels=["left", "bottom"], dms=True, linestyle=":")
    grid.xformatter = LONGITUDE_FORMATTER
    grid.yformatter = LATITUDE_FORMATTER

fig, ax = plt.subplots(figsize=(10, 6), subplot_kw={"projection": data_proj})

make_nice_map(ax)

contour_handle = ax.contourf(
    datatree["geolocation/longitude"],
    datatree["geolocation/latitude"],
    good_array,
    levels=30,
    vmin=0,
    vmax=float(good_array.max()),
    alpha=0.6,
    cmap="inferno",
    zorder=2,
)

cb = plt.colorbar(contour_handle)
cb.set_label(label_from_attrs(data_array))

plt.show()

print("✓ Map created successfully!")
print("✓ Tutorial complete - you've successfully accessed and visualized TEMPO data!")
