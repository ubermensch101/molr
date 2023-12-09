
USAGE="./load_data.sh <host> <user> <password> <dbname> <base_path>"


host="$1"
user="$2"
password="$3"
dbname="$4"
base_path="$5/"
toggle="$6"

# base_path='/home/abhishek/Documents/btp/515_Aurangabad/04138_Khuldabad'
# toggle = 	0 for giving the village path,
#  			1 for giving list of villages i.e. taluka path
# 			2 for giving list of talukas i.e. district path 
# 			3 for giving list of districts i.e. state path

create_schema(){
	echo "Creating schema ${1}"

	psql -q -d ${dbname} -U ${user} -c "create schema if not exists \"${1}\""
	if [ $? -ne 0 ];
	then
		echo "cannot create schema ${1}"
		exit 1
	fi
}

load_akarbandh(){
	schema=$1
	akarbandh_path=$2
	vincode=$3
	echo "Creating Akarbandh table ${schema}.akarbandh"
	psql -q -d ${dbname} -U ${user} \
	-c "drop table if exists "${schema}".akarbandh;"
	
	psql -q -d ${dbname} -U ${user} \
	-c "create table "${schema}".akarbandh (id integer,survey_no integer,area float,vincode integer);"
	if [ $? -ne 0 ];
	then
		echo "Failed to create table ${schema}.akarbandh"
		exit 1
	fi
	echo "Loading akarbandh data"
    echo $akarbandh_path
	find "${akarbandh_path}" -name '*.csv' \
		-exec psql -d ${dbname} -U ${user} \
			-c "\copy "${schema}".akarbandh (id, survey_no, area) from '{}' with (format csv, header true)" \;
	if [ $? -ne 0 ];
			then
				echo "aborting at loading akarbandh data"
				exit 1
		fi

	echo "Updating village census code in Akarbandh table"
	psql -d ${dbname} -U ${user} \
	-c "update
		${schema}.akarbandh
	set
		vincode = ${vincode}"
}


load_GCPs(){
	schema=$1
	gcp_path=$2
	vincode=$3
	echo "Creating GCP table ${schema}.gcp"

	psql -q -d ${dbname} -U ${user} \
	-c "drop table if exists "${schema}".gcp;"
	
	psql -q -d ${dbname} -U ${user} \
	-c "create table "${schema}".gcp (
		point text,
		geom public.geometry(Point, 32643),
		height numeric,
		remark text,
		Lat numeric,
		long numeric,
		vincode integer
	);"
	if [ $? -ne 0 ];
	then
		echo "Failed to create table ${schema}.gcp"
		exit 1
	fi


	find "${gcp_path}" -name '*.csv' \
		-exec psql -d ${dbname} -U ${user} \
			-c "\copy ${schema}.gcp (point, lat, long, height, remark) from '{}' with (format csv, header true)" \;
	if [ $? -ne 0 ];
			then
				echo "aborting at loading GCP data"
				exit 1
		fi

	echo "Updating geometry column GCP table"
	psql -d ${dbname} -U ${user} \
	-c "update ${schema}.gcp set vincode = ${vincode},
		geom = 'Point('||long||' '||lat||')';"
	if [ $? -ne 0 ];
			then
				echo "aborting GCP table updation"
				exit 1
		fi


}

load_cadastrals(){
	schema=$1
	cadastral_path="$2/"
	echo -n "Loading cadastrals: "
for p in $(find "${cadastral_path}" -name '*.shp' -exec ogrinfo -q {} \; | \
			   grep 'Polygon)$' | awk '{print $2}');
do
	echo "shapefile name ${p}.shp"
	find "${cadastral_path}" -name ${p}.shp \
		 -exec ogr2ogr "PG:dbname='${dbname}' host='${host}' user='${user}' \
				password='${password}'" "{}" \
			-lco OVERWRITE=YES -lco GEOMETRY_NAME=geom \
			-lco schema=${schema} -lco SPATIAL_INDEX=GIST \
			-lco FID=gid -nlt PROMOTE_TO_MULTI \
			-nln cadastrals \;
	
	if [ $? -ne 0 ];
	then
		echo "aborting at loading cadastrals"
		exit 1
	fi
done
}

load_GCPs_shp(){
	schema=$1
	gcp_path="$2/"
	echo -n "Loading gcps: "
for p in $(find "${gcp_path}" -name '*.shp' -exec ogrinfo -q {} \; | \
			   grep 'Point)$' | awk '{print $2}');
do
	echo "shapefile name ${p}.shp"
	find "${gcp_path}" -name ${p}.shp \
		 -exec ogr2ogr "PG:dbname='${dbname}' host='${host}' user='${user}' \
				password='${password}'" "{}" \
			-lco OVERWRITE=YES -lco GEOMETRY_NAME=geom \
			-lco schema=${schema} -lco SPATIAL_INDEX=GIST \
			-lco FID=gid -nlt PROMOTE_TO_MULTI \
			-nln gcp \;
	
	if [ $? -ne 0 ];
	then
		echo "aborting at loading gcps"
		exit 1
	fi
done
}


load_survey_plots(){
	schema=$1
	survey_plot_path=$2
	echo -n "Loading Survey plots: "
	for p in $(find "${survey_plot_path}" -name '*.shp' -exec ogrinfo -q {} \; | \
				grep 'Polygon)$' | awk '{print $2}');
	do
		echo "shapefile name ${p}.shp"
		find "${survey_plot_path}" -name ${p}.shp \
			-exec ogr2ogr "PG:dbname=${dbname} host=${host} user=${user} \
			password=${password}" "{}" \
			-lco OVERWRITE=YES -lco GEOMETRY_NAME=geom \
			-lco schema=${schema} -lco SPATIAL_INDEX=GIST \
			-lco FID=gid -nlt PROMOTE_TO_MULTI \
			-nln survey_original \;
		
		if [ $? -ne 0 ];
		then
			echo "aborting at loading survey plots"
			exit 1
		fi
	done
}

load_data_a1(){
    DATABASE_NAME="$1"
    TABLE_NAME="$2"
    shapefile_location="$3"
    shp2pgsql -I -s 4326 "$shapefile_location" "$TABLE_NAME" | psql -U "postgres" -q -d "$DATABASE_NAME"
}


load_data_a2(){
    DATABASE_NAME="$1"
    TABLE_NAME="$2"
    shapefile_location="$3"
    shp2pgsql -a -s 4326 "$shapefile_location" "$TABLE_NAME" | psql -U "postgres" -q -d "$DATABASE_NAME"
}

# load_tippan(){
# 	echo "Loading Tippans"

# 	psql -q -d ${dbname} -U ${user} \
# 	-c "drop table if exists "${schema}".tippan;"
	
# 	a=1
# 	b=1

# 	for i in $(find "$tippan_path" -iname '*.shp');do


# 	if [ $a == $b ]
# 	then
# 	load_data_a1 "govt" "$schema.tippan" $i
# 	else
		
# 	load_data_a2 "govt" "$schema.tippan" $i
# 	fi
# 	let "a=a+1" 
# 	done


# } 

find "$base_path" -maxdepth $toggle -mindepth $toggle -type d | while read dir;
do	
	echo ""
	directory="${dir%/}"
	village_directory_raw="${directory##*/}"
	echo "Village Directory: ${village_directory}"
	village=${directory##*_} 			#getting the suffix of _ in {vincode}_{village_name}
	village_clean=${village,,} #converting to lower case
	schema="${village_clean// /}" #removing spaces
	echo "Schema: ${schema}"
	vincode=${village_directory%%_*} #prefix of _
	echo "VINCODE ${vincode}"
	curr_path="$directory"
	create_schema $schema
	

	#####USE the below function IF GCP is given in csv file format#####
	# gcp_path=$(find "$base_path$folder/" -maxdepth 1 -mindepth 1 -iname "14*" -type d)
	# if [[ "$gcp_path" != "" ]]
	# then load_GCPs "$schema" "$gcp_path" $vincode
	# fi
	#####USE the below function if IF GCP is given in shp file format#####
    gcp_path_shp=$(find "$curr_path/" -maxdepth 1 -mindepth 1 -iname "14*" -type d)
	if [ "$gcp_path_shp" != "" ]
	then load_GCPs_shp "$schema" "$gcp_path_shp"
	fi
    cadastral_path=$(find "$curr_path/" -maxdepth 1 -mindepth 1 -iname "16*" -type d)
	if [ "$cadastral_path" != "" ]
	then load_cadastrals "$schema" "$cadastral_path"
	fi

	survey_path=$(find "$curr_path/" -maxdepth 1 -mindepth 1 -iname "09*" -type d)
	if [ "$survey_path" != "" ]
	then load_survey_plots "$schema" "$survey_path"
	fi
	akarbandh_path=$(find "$curr_path/" -maxdepth 1 -mindepth 1 -iname "15*" -type d)
	if [[ "$akarbandh_path" != "" ]]
	then echo "loading akarbandh"
		find "${akarbandh_path}" -name '*.xlsx' \
		-exec python3 'dataprep/load_akarbandh.py' -db $dbname -host $host -u $user -p $password -v $schema -path '{}' \;
	fi

	# tippan_path=$(find "$base_path$folder/" -maxdepth 1 -mindepth 1 -iname "01*" -type d)
	# if [[ "$tippan_path" != "" ]]
	# then load_tippan "$schema" "$tippan_path"
	# fi
	python3 'dataprep/clean_data.py'  -db $dbname -host $host -u $user -p $password -v $schema
	python3 'dataprep/preliminary_analysis.py'  -db $dbname -host $host -u $user -p $password -v $schema
	python3 'dataprep/correct_wrong_data.py'  -db $dbname -host $host -u $user -p $password -v $schema


done


