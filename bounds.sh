path_to_store=$1
village=$2

ogr2ogr -f "KML" $path_to_store/$village.bounding_box.kml PG:"host='localhost' user='postgres' dbname='dolr' password='postgres'" \
-sql "SELECT st_transform(st_expand(st_transform(st_union(geom),32643),150),4326) AS geom FROM $village.cadastrals"