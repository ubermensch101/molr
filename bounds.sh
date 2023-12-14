village=$1

echo -n "Creating Bounding box: "
mkdir $village.bounding_box
ogr2ogr -f "ESRI Shapefile" ./$village.bounding_box/bounding_box.shp PG:"host='localhost' user='postgres' dbname='dolr' password='postgres'" \
-sql "SELECT st_expand(st_transform(st_union(geom),32643),150) AS geom FROM $village.cadastrals"

zip -q -T -m $village.bounding_box.zip $village.bounding_box/* && echo "success" || echo "failure"
rm -r $village.bounding_box