Structure:
- All relevant constants are stored in config/, and are accessed via the Config class.
- The config file's default parameters can be replaced by giving arguments during Config init.
- Each file must be runnable on its own, and hence must have a if __name__=="main" section.
- Each module (such as data_loading) must have a main file, with helper files located in scripts/.
- Each main file of a module should have a run function.

Installation flow: always at root
1. Create a venv using "python3 -m venv venv"
2. Activate venv usin "source venv/bin/active"
3. pip install the required packages:
- scipy
- pandas
- psycopg2-binary
- openpyxl
- thin-plate-spline
- scikit-learn
- opencv-python
4. pip3 install -e .
