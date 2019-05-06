#!/usr/bin/env python3

# IMPORT OF EXTERNAL LIBRARIES
import os
import sys
import urllib3
import time
import csv
import json
import serial
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
from pathlib import Path
from simple_flock import SimpleFlock

# Change directory to path of this file
os.chdir(os.path.dirname(os.path.realpath(__file__)))

# CONFIG
# PORT = '/dev/tty.usbmodem1411'
# PORT = COM6
PORT = '/dev/ttyUSB3'
proxy = '172.18.140.8:8080'
proxy_dev = '172.18.140.8:8081'
http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=2.0, read=2.0), retries=urllib3.Retry(2, redirect=2))
# If started with this as the dbkey, json dumps will be put in /tmp to be read by the NI-Toolkit
RESTAPI = "restapi"
# Where to find the output of the GPS script
GPS_INPUT_FILE = "/tmp/gps_data.json"
# LOCK FILE (to make sure this script only has 1 running instance
LOCK_FILE = "/tmp/sanitrax_mb.lock"
# Lock timeout in second
LOCK_TIMEOUT = 5.0

# Substructed raw value for correct scaling
RAW_FACTOR = 4630
# Raw values interval between equilevant -1 to 0 bars
RAW_DIF = 14050

# Raw value for 60 degrees (absence of calculation origin)
RAW_TEMP_60 = 8000

# Actual temperature value in Celsius degrees value equilavent to 8000
TEMP = 60

# STRING TUPLES AND DICTIONARIES
modbus_keys = ('mb_magic',  # Used for API version (R) and special functions (W)
               'mb_hydrophore_postrun',  # Time to run after pressure has been reached
               'mb_hydrophore_timeout',  # Maximum time to reach pressure
               'mb_breaktank_delay',  # Delay for exiting error states in the breaktank
               'mb_breaktank_fill_timeout',  # Maximum time for breaktank to go from empty to low
               'mb_pump_start',  # Vacuum pressure value for pump start
               'mb_pump_stop',  # Vacuum pressure value for pump stop
               'mb_pump_throttle_start',  # Vacuum pressure value for pump to start throttling down
               'mb_pump_throttle_stop',  # Vacuum pressure value for pump to stop throttling down
               'mb_pump_timeout',  # Maximum time to reach vacuum pressure
               'mb_pump_max_runtime',  # Maximum runtime at high power (fault if reached)
               'mb_pump_max_temp',  # Maximum pump temperature
               'mb_flush_start',  # Temperature value for flush valve open
               'mb_flush_stop',  # Temperature value for flush valve close
               'mb_flush_timeout',  # Maximum time to reach low temperature
               'mb_antifreeze_dose_1',  # Dosing concentration in % when external_temp < temp_1, set by user
               'mb_antifreeze_dose_2',  # Dosing concentration in % when external_temp < temp_2, set by user
               'mb_antifreeze_dose_3',  # Dosing concentration in % when external_temp < temp_3, set by user
               'mb_antifreeze_dose_4',  # Dosing concentration in % when external_temp < temp_4, set by user
               'mb_antifreeze_dose_5',  # Dosing concentration in % when external_temp < temp_5, set by user
               'mb_antifreeze_dose_manual',  # Manual dosing concentration in %
               'mb_antifreeze_temp_1',  # temperature limit for low value
               'mb_antifreeze_temp_2',  # temperature limit lower value
               'mb_antifreeze_temp_3',  # temperature limit lower value
               'mb_antifreeze_temp_4',  # temperature limit lower value
               'mb_antifreeze_temp_5',  # temperature limit lowest value
               'mb_dosingpump_factor',  # Dosing pump  capacity in liters per hour
               'mb_watermeter_factor',  # Pulse per liter
               'mb_reset_breaktank',  # Set to 1 for exiting timeout state
               'mb_reset_hydrophore',  # Set to 1 for exiting timeout state
               'mb_reset_pump_1',  # Set to 1 for reset procedure and or exit timeout state
               'mb_reset_pump_2',  # Set to 1 for reset procedure and or exit timeout state
               'mb_fault_mask',  # Can be changed to disable specific alarms from triggering main alarm
               'mb_fault',  # 16-bit fault register
               'mb_input_top',  # IO related
               'mb_input_bottom',
               'mb_output',
               'mb_output_mask',
               'mb_output_fault',
               'mb_water_counter',  # Pulse counter
               'mb_dose_counter',   # Dosing counter
               'mb_current_dose',   # Current dosing percentage
               'mb_pcb_temp',  # Temperature of onboard sensor x0.1 degrees Kelvin
               'mb_external_temp',  # Environment temperature
               'mb_external_temp_float_a',  # Part A of 32-bit float point value
               'mb_external_temp_float_b',  # Part B of 32-bit float point value
               'mb_bstate',  # Breaktank state
               'mb_hstate',  # Hydrophore
               'mb_p1state',  # Pump 1
               'mb_p2state',  # Pump 2
               'mb_f1state',  # Flush Valve 1
               'mb_f2state',  # Flush Valve 2
               'mb_dstate',   # Dose pump
               'mb_p1_mbcode',  # Modbus error code for pump 1 (0 = OK)
               'mb_p1_Pot',  # Pump variables
               'mb_p1_Temp',
               'mb_p1_Vac',
               'mb_p1_State',
               'mb_p1_Freq',
               'mb_p1_PID',
               'mb_p1_Fault',
               'mb_p1_Status',
               'mb_p1_Freq_Output',
               'mb_p1_Freq_Ramp',
               'mb_p1_Motor_Current',
               'mb_p1_Motor_Torque',
               'mb_p1_Ext_Status_Word',
               'mb_p1_Mains_Volt',
               'mb_p1_Motor_Volt',
               'mb_p1_Drive_Therm_State',
               'mb_p1_3210',
               'mb_p1_Motor_Power',
               'mb_p2_mbcode',  # Modbus error code for pump 2 (0 = OK)
               'mb_p2_Pot',
               'mb_p2_Temp',
               'mb_p2_Vac',
               'mb_p2_State',
               'mb_p2_Freq',
               'mb_p2_PID',
               'mb_p2_Fault',
               'mb_p2_Status',
               'mb_p2_Freq_Output',
               'mb_p2_Freq_Ramp',
               'mb_p2_Motor_Current',
               'mb_p2_Motor_Torque',
               'mb_p2_Ext_Status_Word',
               'mb_p2_Mains_Volt',
               'mb_p2_Motor_Volt',
               'mb_p2_Drive_Therm_State',
               'mb_p2_3210',
               'mb_p2_Motor_Power',
               'mb_custom_run',  # Custom modbus command (R/W)
               'mb_custom_slave',
               'mb_custom_command',
               'mb_custom_address',
               'mb_custom_quantity',
               'mb_custom_data0',
               'mb_custom_data1',
               'mb_custom_data2',
               'mb_custom_data3',
               'mb_custom_data4',
               'mb_custom_data5',
               'mb_custom_data6',
               'mb_custom_data7',
               'mb_custom_data8',
               'mb_custom_data9',
               'mb_custom_data10',
               'mb_custom_data11',
               'mb_custom_data12',
               'mb_custom_data13',
               'mb_custom_data14',
               'mb_custom_data15')

input_top_keys = ('DC_OK_12V',
                  'DC_OK_24V',
                  'Tank_Low',
                  'Tank_Normal',
                  'Tank_High',
                  'Tank_Overflow',
                  'Pressure_Switch',
                  'Water_Counter',
                  'Anti_Freeze',
                  'Flush_Valve_2',
                  'Flush_Valve_1',
                  'Fill_Tank_Valve',
                  'Dose_Switch_1',
                  'Dose_Switch_2',
                  'Hydrophore_OK',
                  'Emergency_Stop_OK')

input_bottom_keys = ('Vacuum_Pump_2_green',
                     'Vacuum_Pump_2_red',
                     'Vacuum_Pump_1_green',
                     'Vacuum_Pump_1_red',
                     'Hydrophore_Run',
                     'Hydrophore_Red',
                     'Alarm_Lamp',
                     'Reset_Alarm',
                     'Hydrophore_Manual',
                     'Hydrophore_Auto',
                     'Pump1_Manual',
                     'Pump1_Auto',
                     'Pump2_Manual',
                     'Pump2_Auto',
                     'Flush_Button_1',
                     'Flush_Button_2')

output_keys = ('b0',
               'b1',
               'b2',
               'b3',
               'Fill_Tank_Valve',
               'Flush_Valve_1',
               'Flush_Valve_2',
               'Anti_Freeze',
               'b8',
               'Alarm_Lamp',
               'Hydrophore_Red',
               'Hydrophore_Run',
               'Vacuum_Pump_1_red',
               'Vacuum_Pump_1_green',
               'Vacuum_Pump_2_red',
               'Vacuum_Pump_2_green')

fault_keys = ('Water_Supply_Error',
              'Tank_Empty',
              'Tank_Overflow',
              'Hydrophore_Fail',
              'Hydrophore_Timeout',
              'Pump1_Comm_Error',
              'Pump2_Comm_Error',
              'Pump1_Fault',
              'Pump2_Fault',
              'Pump1_Timeout',
              'Pump2_Timeout',
              'Pump1_Overheat',
              'Pump2_Overheat',
              'Emergency_Stop',
              'Dosing_Error',
              'Temp_Sensor_Error')

bstate = ('BREAKTANK_OFF',
          'BREAKTANK_MANUAL',
          'BREAKTANK_FAIL',
          'BREAKTANK_AUTO_EMPTY',
          'BREAKTANK_AUTO_LOW',
          'BREAKTANK_AUTO_NORMAL',
          'BREAKTANK_AUTO_HIGH',
          'BREAKTANK_AUTO_OVERFLOW',
          'BREAKTANK_AUTO_TIMEOUT')

hstate = ('HYDROPHORE_OFF',
          'HYDROPHORE_MANUAL',
          'HYDROPHORE_FAIL',
          'HYDROPHORE_AUTO_OFF',
          'HYDROPHORE_AUTO_ON',
          'HYDROPHORE_AUTO_POSTRUN',
          'HYDROPHORE_AUTO_TIMEOUT')

pstate = ('PUMP_OFF',
          'PUMP_MANUAL',
          'PUMP_FAIL',
          'PUMP_AUTO_OFF',
          'PUMP_AUTO_ON',
          'PUMP_AUTO_TIMEOUT')

fstate = ('FLUSH_OFF',
          'FLUSH_MANUAL',
          'FLUSH_AUTO_ON',
          'FLUSH_AUTO_TIMEOUT')

dstate = ('DOSE_OFF',
          'DOSE_FIXED_OFF',
          'DOSE_FIXED_ON',
          'DOSE_AUTO_OFF',
          'DOSE_AUTO_ON',
          'DOSE_ALWAYS_ON',
          'DOSE_ALWAYS_ON_PAUSE')

pump_fault_dict = {'-3': 'Pump Overheat',
                   '-2': 'Pump Timeout',
                   '-1': 'No communication',
                   '0': 'OK',
                   '2': 'Control Eeprom (EEF1)',
                   '3': 'Incorrect config. (CFF)',
                   '4': 'Invalid config. (CFI)',
                   '5': 'Modbus com. (SLF1)',
                   '6': 'int. com.link (ILF)',
                   '7': 'Com. network (CnF)',
                   '8': 'External flt-LI/Bit (EPF1)',
                   '9': 'Overcurrent (OCF)',
                   '10': 'Precharge (CrF)',
                   '11': 'Speed fdback loss (SPF)',
                   '12': 'Load slipping (AnF)',
                   '16': 'Drive overheat (OHF)',
                   '17': 'Motor overload (OLF)',
                   '18': 'Overbraking (ObF)',
                   '19': 'Mains overvoltage (OSF)',
                   '20': '1 output phase loss (OPF1)',
                   '21': 'Input phase loss (PHF)',
                   '22': 'Undervoltage (USF)',
                   '23': 'Motor short circuit (SCF1)',
                   '24': 'Overspeed (SOF)',
                   '25': 'Auto-tuning (tnF)',
                   '26': 'Rating error (InF1)',
                   '27': 'PWR Calib. (InF2)',
                   '28': 'Int.serial link (InF3)',
                   '29': 'Int.Mfg area (InF4)',
                   '30': 'Power Eeprom (EEF2)',
                   '32': 'Ground short circuit (SCF3)',
                   '33': '3out ph loss (OPF2)',
                   '34': 'CAN com. (COF)',
                   '35': 'Brake control (bLF)',
                   '38': 'External fault com. (EPF2)',
                   '41': 'Brake feedback (brF)',
                   '42': 'PC com. (SLF2)',
                   '44': 'Torque/current lim (SSF)',
                   '45': 'HMI com. (SLF3)',
                   '49': 'LI6=PTC probe (PtFL)',
                   '50': 'PTC fault (OtFL)',
                   '51': 'Internal- I measure (InF9)',
                   '52': 'Internal-mains circuit (InFA)',
                   '53': 'Internal- th. sensor (InFb)',
                   '54': 'IGBT overheat (tJF)',
                   '55': 'IGBT short circuit (SCF4)',
                   '56': 'Motor short circuit (SCF5)',
                   '58': 'Out. contact. stuck (FCF1)',
                   '59': 'Out. contact. open. (FCF2)',
                   '64': 'input contactor (LCF)',
                   '67': 'IGBT desaturation (HdF)',
                   '68': 'Internal-option (InF6)',
                   '69': 'internal- CPU (InFE)',
                   '71': 'AI3 4-20mA loss (LFF3)',
                   '73': 'Cards pairing (HCF)',
                   '76': 'Load fault (dLF)',
                   '77': 'Bad conf (CFI2)',
                   '99': 'Ch.sw. fault (CSF)',
                   '100': 'Pr.Underload.Flt (ULF)',
                   '101': 'Proc.Overload Flt (OLC)',
                   '105': 'Angle error (ASF)',
                   '107': 'Safety fault (SAFF)',
                   '108': 'FB fault (FbE)',
                   '109': 'FB stop flt. (FbES)'}

gpsDict = {'Latitude': '',
           'Longitude': '',
           'Time': '',
           'Altitude': ''}


Pump_Status = ('Reserved',
               'Ready',
               'Running',
               'Fault',
               'Power section line supply present',
               'Reserved',
               'Reserved',
               'Alarm',
               'Reserved',
               'Command via Network',
               'Reference reached',
               'Reference outside limits',
               'Reserved',
               'Reserved',
               'STOP key pressed',
               'Reverse rotation')

AntiFreeze_Status = ('Dose 1',
                     'Dose 2',
                     'Dose 3',
                     'Dose 4',
                     'Dose 5',
                     'Fixed Dose',
                     'Temperature 1',
                     'Temperature 2',
                     'Temperature 3',
                     'Temperature 4',
                     'Temperature 5',
                     'Dosing pump factor',
                     'Water meter factor',
                     'Water counter',
                     'Dose counter',
                     'Current dose')

Temperature = ('External temperature',
               'External temperature A',
               'External temperature B')


def load_settings():
    with open('settings.json', 'r') as fp:
        return json.load(fp)


def save_settings(settings):
    with open('settings.json', 'w') as fp:
        json.dump(settings, fp, sort_keys=True, indent=4)
        return "OK"


def read_gps_data_from_file():
    try:
        with open(GPS_INPUT_FILE, "r") as fp:
            return json.load(fp)
    except OSError:
        return {
            "Latitude": "Unknown",
            "Longitude": "Unknown",
            "Time": "1900-01-01 00:00:00"
        }


def water_counter(pulse):
    water_dict = {
        "sum": 0,
        "previous": 0
    }
    try:
        with open('WaterCounter.json', 'r') as fp:
            data = json.load(fp)
            water_dict = {
                "sum": data["sum"],
                "previous": data["previous"]
            }
    except:
        pass

    if (pulse < water_dict["previous"]):
        water_dict["previous"] = 0

    water_dict["sum"] += pulse - water_dict["previous"]
    water_dict["previous"] = pulse

    with open('WaterCounter.json', 'w') as fp:
            json.dump(water_dict, fp, sort_keys=True, indent=4)

    return water_dict['sum']


def modbus_read(address, amount):
    # Reads "amount" registers starting from address "address" from slave device 1
    try:
        master = modbus_rtu.RtuMaster(
            serial.Serial(port=PORT, baudrate=9600, bytesize=8, parity='N', stopbits=1, xonxoff=0))
        master.set_timeout(1)
        master.set_verbose(True)
        modbus_values = master.execute(1, cst.READ_HOLDING_REGISTERS, address, amount)

        if debug > 0:
            print("Read modbus values:")
            print(modbus_values)
        return modbus_values

    except:
        print("Can not connect to modbus slave\n")
        modbus_values = "error"
        # Sample modbus values for debugging
        # modbus_values = 1, 60, 900, 10, 30, 15870, 10250, 13000, 11500, 30, 1200, 8000, 7000, 6750, 6000, 10, 20, 30,\
        #                40 ,50, 50, 5, 0, 65536-5, 65536-15 ,65536-25, 0, 0, 0, 0, 0, 0, 65535, 0, 49247, 10752, 0, 65276, 0, 0 , 0, 0,\
        #                1223, 11, 11, 11, 0, 6, 3, 3, 3, 0, 2, 0, 0, 0, 6584, 15443, 0, 350, 0, 5, 2611, 0, 350, 0, 0, 2, 4722,\
        #                0, 32, 32768,0 , 0, 1057, 6384, 15425, 0, 350, 0, 5, 2611, 0, 350, 0, 0, 2, 4717, 0, 30, 32768, 0
        return modbus_values


def modbus_write(address, value):
    # Writes value(s) to a modbus register (starting address) of slave device 1.
    # "value" can be of type "int" or "list", for writing a single or multiple registers.
    try:
        master = modbus_rtu.RtuMaster(
            serial.Serial(port=PORT, baudrate=9600, bytesize=8, parity='N', stopbits=1, xonxoff=0))
        master.set_timeout(1)
        master.set_verbose(True)
        if type(value) is int:
            if debug > 0:
                print("Writing ", value ," to address", address)
            result = master.execute(1, cst.WRITE_SINGLE_REGISTER, address, output_value=value)
        elif type(value) is list:
            if debug > 0:
                print("Writing ", len(value), " registers to address ", address)
            result = master.execute(1, cst.WRITE_MULTIPLE_REGISTERS, address, output_value=value)
        else:
            print("Modbus write error, invalid value type: ", type(value))
        return result

    except:
        return "error"


# Function to make HTTP post with JSON content
def http_post_json(target, command, data):
    try:
        encoded_data = json.dumps(data).encode('utf-8')
        r = http.request('POST', target + command, body=encoded_data, headers={'Content-Type': 'application/json'})
        return r.status
    except:
        print("No connection to IP:", target)
        return 0


# Function to make HTTP call with JSON content
def http_get_json(target, command):
    try:
        r = http.request('GET', target + command)
        return json.loads(r.data.decode('utf-8'))
    except:
        print("No connection to IP:", target)
        return 0


def write_log(filename, logheader, logdata):
    # In the directory of this python file there should be a subdirectory or a symlink called "log"
    # where log files will be stored in separate CSV files per day, using UTC timestamps.
    logdate = time.strftime("%Y-%m-%d", time.gmtime())
    logtime = time.strftime("%H:%M:%S", time.gmtime())
    logfile = 'log/' + logdate + "_" + filename + '.csv'

    # If the file does not exists, also a header will be written
    if not Path(logfile).is_file():
        with open(logfile, 'a') as fp:
            header = ["Date", "Time (UTC)"]
            header.extend(logheader)
            writer = csv.writer(fp, delimiter=";", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)

    with open(logfile, 'a') as fp:
        data_row = [logdate, logtime]
        data_row.extend(logdata)
        writer = csv.writer(fp, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(data_row)


def writeDictAsJsonData(data, filename):
    json_data = json.dumps(data, indent=4)
    filehandler = open("/tmp/" + filename + ".json", 'w')
    print(json_data, file=filehandler)
    filehandler.close()


# Calculate the maximum temperature for 16 bits
MAX_TEMPERATURE = pow(2, 16)


# Modbus will return 2 complement values for values < 0, so this needs correction
# @param temperature The temperature value to correct
# @returns Returns a valid temperature
def fixNegativeTemperature(temperature):

    # This temp should never be reached, this is likely to be a 2complement value
    if temperature > (MAX_TEMPERATURE / 2) - 1:
        temperature -= MAX_TEMPERATURE

    return temperature


# Make sure the negative temperatures are packed as a two complement
# (which will be the result for any negative value returned by modbus)
def prepareData(modbus_values):
    # Convert tuple (read only) to a list
    values = list(modbus_values)

    # Temperature fix(es)
    external_temp_index = modbus_keys.index("mb_external_temp")
    values[external_temp_index] = fixNegativeTemperature(values[external_temp_index] )
    values[external_temp_index] /= 10

    # Convert (possible) negative 2 complement value to normal value
    start_index = modbus_keys.index("mb_antifreeze_temp_1")
    end_index = modbus_keys.index("mb_antifreeze_temp_5")
    while start_index <= end_index:
        values[start_index] = fixNegativeTemperature(values[start_index])
        start_index += 1

    return tuple(values)


def main(mode):
    modbus_values = modbus_read(0, 95)
    if modbus_values == "error":
        quit()

    if mode == "firebase":
        # Write the raw data to a log file (without any alterations)
        write_log("Sanitrax", modbus_keys[:95], modbus_values)
    # Correct values before use and store comparison
    modbus_values = prepareData(modbus_values)

    # Load settings from file and compare to current settings
    # Write settings to controller if not up-to-date
    # If the file does not exist yet, then store the current settings in the controller
    try:
        current_settings = list(modbus_values[1:28])
        store = load_settings()
        stored_settings = []
        for key in modbus_keys[1:28]:
            stored_settings.append(store[key])
        if current_settings != stored_settings:
            if debug > 0:
                print("Settings do not match stored values, updating:")
                print(store)
                print(stored_settings)
                print(current_settings)
            modbus_write(1, stored_settings)
    except:
        print("settings.json does not exist, creating new file")
        current_settings = dict(zip(modbus_keys[1:28], modbus_values[1:28]))
        save_settings(current_settings)

    # Create a dictionary of all the modbus values for easier reference in further processing
    modbus_dict = dict(zip(modbus_keys, modbus_values))
    water_sum = water_counter(modbus_dict['mb_water_counter'])
    gps_dict = read_gps_data_from_file()

    # Create lists of bits from 16-bit registers (reversed order, represented as int with value 0 or 1)
    input_top_val = list(map(int, reversed(bin(modbus_dict['mb_input_top'])[2:].zfill(16))))
    input_bottom_val = list(map(int, reversed(bin(modbus_dict['mb_input_bottom'])[2:].zfill(16))))
    output_val = list(map(int, reversed(bin(modbus_dict['mb_output'])[2:].zfill(16))))
    output_mask = list(map(int, reversed(bin(modbus_dict['mb_output_mask'])[2:].zfill(16))))
    output_fault = list(map(int, reversed(bin(modbus_dict['mb_output_fault'])[2:].zfill(16))))
    fault_val = list(map(int, reversed(bin(modbus_dict['mb_fault'])[2:].zfill(16))))
    fault_mask = list(map(int, reversed(bin(modbus_dict['mb_fault_mask'])[2:].zfill(16))))
    pump1_status = list(map(int, reversed(bin(modbus_dict['mb_p1_Status'])[2:].zfill(16))))
    pump2_status = list(map(int, reversed(bin(modbus_dict['mb_p2_Status'])[2:].zfill(16))))

    # Create dictionaries from keys and values
    input_top_dict = dict(zip(input_top_keys, input_top_val))
    input_bottom_dict = dict(zip(input_bottom_keys, input_bottom_val))
    output_dict = dict(zip(output_keys, output_val))
    output_mask_dict = dict(zip(output_keys, output_mask))
    output_fault_dict = dict(zip(output_dict, output_fault))
    fault_dict = dict(zip(fault_keys, fault_val))
    fault_mask_dict = dict(zip(fault_keys, fault_mask))
    pump1_status_dict = dict(zip(Pump_Status, pump1_status))
    pump2_status_dict = dict(zip(Pump_Status, pump2_status))
    antifreeze_pump_dict = dict(zip(AntiFreeze_Status, modbus_values[15:28] + modbus_values[39:42]))
    temperature_dict = dict(zip(Temperature, modbus_values[43:46]))

    # Value scaling
    modbus_dict["mb_p1_Vac"] = ((modbus_dict["mb_p1_Vac"] - RAW_FACTOR) / RAW_DIF) - 1
    modbus_dict["mb_p2_Vac"] = ((modbus_dict["mb_p2_Vac"] - RAW_FACTOR) / RAW_DIF) - 1

    modbus_dict["mb_p1_Temp"] = (modbus_dict["mb_p1_Temp"] * TEMP) / RAW_TEMP_60    
    modbus_dict["mb_p2_Temp"] = (modbus_dict["mb_p2_Temp"] * TEMP) / RAW_TEMP_60

    modbus_dict["mb_p1_Motor_Current"] /= 10
    modbus_dict["mb_p2_Motor_Current"] /= 10
 
    modbus_dict["mb_p1_Mains_Volt"] /= 10
    modbus_dict["mb_p2_Mains_Volt"] /= 10

    total_water_usage = water_sum / modbus_dict['mb_watermeter_factor']

    # Create dictionaries that are used to display actual values in the App.
    pump1_display_dict = {"current": modbus_dict["mb_p1_Motor_Current"],
                          "mains_voltage": modbus_dict["mb_p1_Mains_Volt"],
                          "pump_temperature": modbus_dict["mb_p1_Temp"],
                          "relative_pressure": modbus_dict["mb_p1_Vac"],
                          }
    pump2_display_dict = {"current": modbus_dict["mb_p2_Motor_Current"],
                          "mains_voltage": modbus_dict["mb_p2_Mains_Volt"],
                          "pump_temperature": modbus_dict["mb_p2_Temp"],
                          "relative_pressure": modbus_dict["mb_p2_Vac"],
                          }
    hydrophore_display_dict = {"total": total_water_usage}

    # The drives store fault codes in their fault registers, but these are only "active" when the fault bit in the
    # status word is true. We overwrite the fault code with '0' when this bit is not true, indicating "no fault".
    # Internally detected errors (modbus errors, timeout and overheat) get negative fault code values.

    # Check Pump 1 error
    if fault_dict["Pump1_Comm_Error"]:
        modbus_dict["mb_p1_Fault"] = -1
    elif fault_dict["Pump1_Timeout"]:
        modbus_dict["mb_p1_Fault"] = -2
    elif fault_dict["Pump1_Overheat"]:
        modbus_dict["mb_p1_Fault"] = -3
    elif not fault_dict["Pump1_Fault"]:
        modbus_dict["mb_p1_Fault"] = 0

    # Check Pump 2 error
    if fault_dict["Pump2_Comm_Error"]:
        modbus_dict["mb_p2_Fault"] = -1
    elif fault_dict["Pump2_Timeout"]:
        modbus_dict["mb_p2_Fault"] = -2
    elif fault_dict["Pump2_Overheat"]:
        modbus_dict["mb_p2_Fault"] = -3
    elif not fault_dict["Pump2_Fault"]:
        modbus_dict["mb_p2_Fault"] = 0

    p1_dict = pump1_status_dict
    p1_dict["Status_text"] = pump_fault_dict[str(modbus_dict["mb_p1_Fault"])]

    p2_dict = pump2_status_dict
    p2_dict["Status_text"] = pump_fault_dict[str(modbus_dict["mb_p2_Fault"])]

    states_dict = {}
    states_dict["Hydrophore"] = hstate[modbus_dict['mb_hstate']]
    states_dict["Breaktank"] = bstate[modbus_dict['mb_bstate']]
    states_dict["Pump 1"] = pstate[modbus_dict['mb_p1state']]
    states_dict["Pump 2"] = pstate[modbus_dict['mb_p2state']]
    states_dict["Flush Valve 1"] = fstate[modbus_dict['mb_f1state']]
    states_dict["Flush Valve 2"] = fstate[modbus_dict['mb_f2state']]
    states_dict["Antifreeze Pump"] = dstate[modbus_dict['mb_dstate']]

    # Operate in either console, FireBase or RESTAPI mode
    if mode == RESTAPI:
        writeDictAsJsonData(modbus_dict, "modbus")
        writeDictAsJsonData(input_top_dict, "top")
        writeDictAsJsonData(input_bottom_dict, "bottom")
        writeDictAsJsonData(output_dict, "output")
        writeDictAsJsonData(output_mask_dict, "output_mask")
        writeDictAsJsonData(output_fault_dict, "output_fault")
        writeDictAsJsonData(fault_dict, "fault")
        writeDictAsJsonData(fault_mask_dict, "fault_mask")
        writeDictAsJsonData(p1_dict, "pump1")
        writeDictAsJsonData(p2_dict, "pump2")
        writeDictAsJsonData(states_dict, "states")
        writeDictAsJsonData(antifreeze_pump_dict, "antifreeze")
        writeDictAsJsonData(temperature_dict, "temperature")
        writeDictAsJsonData(gps_dict, "gps")
        return

    if mode == "console":
        print(modbus_dict)

        print("\n======== Input Top =============")
        for key, value in input_top_dict.items():
            print(key + ': ', value)

        print(" \n========= Input Bottom =========")
        for key, value in input_bottom_dict.items():
            print(key + ': ', value)

        print(" \n========= Output ===============")
        for key, value in output_dict.items():
            print(key + ': ', value)

        print(" \n========= Output Mask ============")
        for key, value in output_mask_dict.items():
            print(key + ': ', value)

        print(" \n========= Output Fault ============")
        for key, value in output_fault_dict.items():
            print(key + ': ', value)

        print(" \n========= Fault Registers ============")
        for key, value in fault_dict.items():
            print(key + ': ', value)

        print(" \n========= Fault Mask ============")
        for key, value in fault_mask_dict.items():
            print(key + ': ', value)

        print("\n========== Pump 1 Status ==========")
        for key, value in p1_dict.items():
            print(key + ': ', value)

        print("\n========== Pump 2 Status ==========")
        for key, value in p2_dict.items():
            print(key + ': ', value)

        print(" \n========= States ============")
        for key, value in states_dict.items():
            print(key + ': ', value)

        print("\n========== Antifreeze Status ==========")
        for key, value in antifreeze_pump_dict.items():
            print(key + ': ', value)

        print("\n========== Temperature Status ==========")
        for key, value in temperature_dict.items():
            print(key + ': ', value)

        print("\n========== GPS Readout ==========")
        for key, value in gps_dict.items():
            print(key + ': ', value)

    if mode == "firebase":
        # API v2 on production database.
        # Check for reset or setting changes
        try:
            actions = http_get_json(proxy, "/database/modules/" + dbkey + "/settings/actions")
            if actions['resetBreaktank']:
                modbus_write(modbus_keys.index('mb_reset_breaktank'), 1)
                data = {"id": dbkey, "location": "/settings/actions", "value": {"resetBreaktank": False}}
                http_post_json(proxy, '/database/update', data)
            if actions['resetHydrophore']:
                modbus_write(modbus_keys.index('mb_reset_hydrophore'), 1)
                data = {"id": dbkey, "location": "/settings/actions", "value": {"resetHydrophore": False}}
                http_post_json(proxy, '/database/update', data)
            if actions['resetPump1']:
                modbus_write(modbus_keys.index('mb_reset_pump_1'), 1)
                data = {"id": dbkey, "location": "/settings/actions", "value": {"resetPump1": False}}
                http_post_json(proxy, '/database/update', data)
            if actions['resetPump2']:
                modbus_write(modbus_keys.index('mb_reset_pump_2'), 1)
                data = {"id": dbkey, "location": "/settings/actions", "value": {"resetPump2": False}}
                http_post_json(proxy, '/database/update', data)
            if actions['applyChanges']:
                new_settings = http_get_json(proxy, "/database/modules/" + dbkey + "/settings/new")
                current_settings = dict(zip(modbus_keys[1:28], modbus_values[1:28]))
                if new_settings != current_settings:
                    save_settings(new_settings)

                    # Create list in correct order with new settings
                    settings_array = []
                    for key in modbus_keys[1:28]:
                        settings_array.append(new_settings[key])
                    
                    if debug > 0:
                        print("Applying new settings from database:")
                        print(settings_array)
                    modbus_write(1, settings_array)
                    # Then write the new settings as "current" to database
                    data = {"id": dbkey, "location": "settings/current", "value": new_settings}
                    http_post_json(proxy, '/database/update', data)
                data = {"id": dbkey, "location": "/settings/actions", "value": {"applyChanges": False}}
                http_post_json(proxy, '/database/update', data)

        except:
            print("/settings/actions not defined in database")

        api_2_data = {
            "breakTank": {
                "state": bstate[modbus_dict['mb_bstate']],
                "waterCounter": water_sum
            },
            "dosingPump": {
                "state": dstate[modbus_dict['mb_dstate']],
                "doseCounter": modbus_dict['mb_dose_counter'],
                "currentDose": modbus_dict['mb_current_dose'],
            },
            "environment": {
                "gpsLocation": str(gps_dict['Latitude'])+","+str(gps_dict['Longitude']),
                "gpsTimestamp": gps_dict['Time'],
                "temperature": modbus_dict['mb_external_temp']
            },
            "heartbeat": {
                "timestamp": int(time.time()),
            },
            "hydrophore": {
                "error": fault_dict["Hydrophore_Fail"],
                "state": hstate[modbus_dict['mb_hstate']],
                "display": hydrophore_display_dict,
            },
            "pump1": {
                "data": dict(zip(modbus_keys[53:72], modbus_values[53:72])),
                "error": modbus_dict["mb_p1_Fault"],
                "errorDescription": pump_fault_dict[str(modbus_dict["mb_p1_Fault"])],
                "flush": fstate[modbus_dict['mb_f1state']],
                "state": pstate[modbus_dict['mb_p1state']],
                "status": pump1_status_dict,
                "display": pump1_display_dict,
            },
            "pump2": {
                "data": dict(zip(modbus_keys[72:91], modbus_values[72:91])),
                "error": modbus_dict["mb_p2_Fault"],
                "errorDescription": pump_fault_dict[str(modbus_dict["mb_p2_Fault"])],
                "flush": fstate[modbus_dict['mb_f2state']],
                "state": pstate[modbus_dict['mb_p2state']],
                "status": pump2_status_dict,
                "display": pump2_display_dict,
            },
            "system": {
                "fault": fault_dict,
                "inputTop": input_top_dict,
                "inputBottom": input_bottom_dict,
                "output": output_dict,
                "pcbTemperature": modbus_dict["mb_pcb_temp"]
            }
        }

        # Write data to database
        data = {"id": dbkey, "location": "status", "value": api_2_data}
        http_post_json(proxy, '/database/update', data)

        # Write historical data to log
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/waterCounter', water_sum)
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump1Current', modbus_dict["mb_p1_Motor_Current"])
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump1Pressure', modbus_dict["mb_p1_Vac"])
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump1Temperature', modbus_dict["mb_p1_Temp"])
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump1Voltage', modbus_dict["mb_p1_Mains_Volt"])
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump2Current', modbus_dict["mb_p2_Motor_Current"])
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump2Pressure', modbus_dict["mb_p2_Vac"])
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump2Temperature', modbus_dict["mb_p2_Temp"])
        http_post_json(proxy, '/api/v2/modules/' + dbkey + '/history/pump2Voltage', modbus_dict["mb_p2_Mains_Volt"])


if __name__ == "__main__":
    global dbkey
    global debug
    debug = 0
    if len(sys.argv) < 2:
        print("Usage: Sanitrax_CTRL.py key [debug: 0 or 1]")
        print("Running in console mode now")
        main("console")

    else:

        dbkey = sys.argv[1]
        if len(sys.argv) > 2:
            debug = int(sys.argv[2])
        if debug > 0:
            print("dbkey:", dbkey)

        try:
            with SimpleFlock(LOCK_FILE, LOCK_TIMEOUT):
                # HACK: if key = "restapi" then use a special console version
                if dbkey == RESTAPI:
                    main(RESTAPI)
                else:
                    main("firebase")
        except TimeoutError:
            print("Unable to acquire lock, quitting...")
