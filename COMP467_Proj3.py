import re
import csv
import argparse
import pymongo
import sys 
from datetime import datetime
import os
import pandas as pd

import shlex
import subprocess

import openpyxl
from openpyxl.drawing.image import Image

# =============================
# File ingesting and printing
# =============================

# Ingest files and store as 'xytech' and 'baselight'
def read_files(xy_job, jobs):
    with open (xy_job, 'r') as file:
        xytech = file.read()
        file.close()
    with open (jobs, 'r') as file:
        baselight = file.read()
        file.close()
    return xytech, baselight

# Ingest flame files and store as 'flame'
def read_flames(jobs):
    with open (jobs, 'r') as file:
        flame = file.read()
        file.close()
    return flame

# Dictionaries for file directories and frame storing later
global_dictionary = {}
secondary_dict = {}

# ===============================
# Argparse arguments
# ===============================

parser = argparse.ArgumentParser()
parser.add_argument('--files', dest='workFiles', nargs='*', help='Baselight/Flames files to process')
parser.add_argument('--process', dest='vidFiles', nargs='*')
parser.add_argument('--xytech', help='Xytech files to process')
parser.add_argument('--verbose', action='store_true', help='Console output on/off')
parser.add_argument('--outputType', help='CSV, Database, or XLS')
args = parser.parse_args()

output = ''

# READ XYTECH and BASELIGHT FIRST


# ARGPARSE CONDITIONALS
if(args.workFiles is None):
  print("No BL or Flames file(s) selected!")
  # sys.exit(2)
else:
  jobs = args.workFiles

if(args.xytech is None):
  print("No Xytech file selected!")
  # sys.exit(2)
else:
  xy_job = args.xytech

if(args.verbose):
  print("Verbose - output results to console!")
else:
  print("Not Verbose - do not output results to console!")

if(args.outputType == "DB"):
  print("DB Selected - Output to MongoDB!")
  output = 'DB'
if(args.outputType == "CSV"):
  print("CSV Selected - Output to a CSV!")
  output = 'CSV'
if(args.outputType == 'XLS'):
  print("XLS Selected - Output to an XLS with timecodes and mid-thumbnails!")
  output = 'XLS'

# ==================================
# DATA PARSING 
# ==================================
if args.workFiles is not None:
  xytech, baselight = read_files(args.xytech, args.workFiles[0])

  # THEN READ ANY FLAMES FILES AFTER THE BASELIGHT (IF THERE ARE ANY)
  flame_arr = []
  if(len(args.workFiles) > 1):
    for file in args.workFiles[1:]:
      flame = read_flames(file)
      flame = flame.strip()
      no_lines = flame.strip('\n')
      no_lines2 = no_lines.split('\n')
      flame_arr.append(no_lines2)
# ======================
# XYTECH
# ======================

  pattern_producer = re.compile(r'Producer:.+')
  pattern_operator = re.compile(r'Operator:.+')
  pattern_job = re.compile(r'Job:.+')
  pattern_loc = re.compile(r'/.+')
  pattern_notes = re.compile(r'Notes.+\n.+')

  # XYTECH FIND ALL MATCHES
  pat_p = pattern_producer.findall(xytech)
  pat_o = pattern_operator.findall(xytech)
  pat_j = pattern_job.findall(xytech)
  pat_l = pattern_loc.findall(xytech)
  pat_n = pattern_notes.findall(xytech)

  # DICTIONARY APPENDAGE
  prod_split = pat_p[0].split(':')
  oper_split = pat_o[0].split(':')
  job_split = pat_j[0].split(':')
  notes_split = pat_n[0].split(':')

  global_dictionary[prod_split[0]] = prod_split[1].strip()
  global_dictionary[oper_split[0]] = oper_split[1].strip()
  global_dictionary[job_split[0]] = job_split[1].strip()
  global_dictionary[notes_split[0]] = notes_split[1].strip()

  # Filter method: get rid of errors, nulls, or blanks + store in new list
  def err_filter(init_frames):
    nums_only = []
    for j in range(len(init_frames)):
      if(re.match(r'\d', init_frames[j])):
        nums_only.append(init_frames[j])
    return nums_only

  def err_flame_filter(init_frames):
    nums_only = []
    for line in init_frames:
      for frame in line:
        if(re.match(r'\d', frame)):
          nums_only.append(frame)
    return nums_only

  # To integer method: converts list of individual frame strings to ints
  def to_int(f):
    ints_only = []
    for list in f:
      for frame in list:
        ints_only.append(int(frame))
    return ints_only

  # Convert frames into intervals method
  def interval_frames(f):
    i = 0
    data = []

    # Make sure all frames are in increasing order first
    f.sort()

    while(i < len(f)):                                # Always remember, length is always one more index
        start = f[i]                                    # Set start frame
        while((i < len(f)-1) and (int(f[i])+1 == int(f[i+1]))): # While i is less than the last index AND the next frame follows the interval, keep iterating +1
          i+=1                                                  # If there was no index check, once i iterates up again, nums[i+1] would make the whole while condition go out of bounds (IT MUST BE CHECKED FIRST)
        end = f[i]                                      # Once the next frame makes a "jump", set the end to the current frame 
        if(int(end)-int(start) == 0):                         # If start and end are the same frame, print the start (or end, doesn't really matter)
          data.append(f'{start}')
        else:                                                 # Otherwise, print the range of frames
          data.append(f'{start}-{end}')
        i+=1                                                  # Move to the "jumped" frame and restart the new interval
    return data

  # ==============
  # BASELIGHT 
  # ==============

  pattern_hplocs = re.compile(r'(/[^\s]+)')
  pattern_hpframes = re.compile(r'\s(.*)\n')
  # BASELIGHT FIND ALL MATCHES
  locations = pattern_hplocs.findall(baselight)
  frames = pattern_hpframes.findall(baselight)

  # BASELIGHT AND XYTECH REMOVAL REGEX
  locfix_pattern = re.compile(r'/images\d+/')
  xylocfix_pattern = re.compile(r'/ddnsata\d+/production/')
  xyloc_prefix = xylocfix_pattern.findall(xytech)

  xylocfix_pattern2 = re.compile(r'/ddnsata\d+/production')
  xyloc_prefix2 = xylocfix_pattern.findall(xytech)

  f_xy = []
  for i in range(len(pat_l)):
    f_xy.append(re.sub(xylocfix_pattern2, "", pat_l[i])) 

  # Xytech regex directory removal for comparison
  new_xy = []
  for i in range(len(pat_l)):
    new_xy.append(re.sub(xylocfix_pattern, "", pat_l[i])) 
  # Baselight regex directory removal for comparison
  new_base = []
  for i in range(len(locations)):
    new_base.append(re.sub(locfix_pattern, "", locations[i]))  

  # =====================================================
  # New list to store all final and fixed locations

  fixed_locs = []
  counter=0
  for i in range(len(new_base)):  
    while(counter<len(new_xy)):
      if(new_base[i] == new_xy[counter]):     # If the baselight location matches one of the xytech locations (1 out of however many in that xytech)
        temp = xyloc_prefix[counter] + new_base[i]
        fixed_locs.append(temp)               # Append the newly created location and set the counter to 6 to exit the while loop
        counter=len(new_xy)
      else:
        counter+=1                            # If the current baselight loc doesn't match the current xytech loc, iterate counter by 1
    counter=0                                 # Reset counter for the next location

  # =====================================================
  # Filter invalid frames + convert frames into ints

  baselight_int_list = []
  for i in range(len(frames)):
    temp = []
    # First filter out the errors and store/save
    temp.append(err_filter((frames[i].split(" "))))
    # Then append the new ints to the main return list
    baselight_int_list.append(to_int(temp))

  # ==============================================================================================================
  # Append final locations as keys and frames (as integers) as their respective content into global dictionary
  # 4/14: This was adjusted to NOT calculate ranges yet, in case there are more flame files to add first

  if output == 'CSV':
    for i in range(len(baselight_int_list)):
      if(fixed_locs[i] in global_dictionary): # If the location is already in the dictionary, add the next frames instead of completely replacing the old ones
        global_dictionary[fixed_locs[i]] = global_dictionary.get(fixed_locs[i]) + baselight_int_list[i]
      else:   # Otherwise, add it the normal "replacing way"
        global_dictionary[fixed_locs[i]] = baselight_int_list[i]

  if output == 'DB':
    for i, frame_arr in enumerate(baselight_int_list):
      str_frames = ' '.join(interval_frames(frame_arr))
      if jobs[0] in secondary_dict:
        secondary_dict[jobs[0]].append(f'{fixed_locs[i]} {str_frames}')
      else:
        secondary_dict[jobs[0]] = [f'{fixed_locs[i]} {str_frames}']

  # =========================
  # FLAMES PARSING (IF THERE ARE ANY)
  # =========================
  if len(args.workFiles) >= 2:

    flame_files_only = []
    for file in jobs[1:]:
      flame_files_only.append(file)

    flame_split = []
    for file in flame_arr:
      for line in range(len(file)):
        flame_split.append(file[line].split(" ")[1:])

    # Extract the frames and store them into their respective indexes/blocks
    flame_frames = []
    for file in flame_arr:
      for line in range(len(file)):
        flame_frames.append(file[line].split(" ")[2:])

    # =========================================================
    # Append correct 'ddnsata' prefixes to each flame location

    fixed_flame_locs = []
    c=0
    for i in range(len(flame_split)):
      while(c<len(new_xy)):
        if(flame_split[i][0] == new_xy[c]):
          temp = xyloc_prefix[c] + flame_split[i][0]
          fixed_flame_locs.append(temp)
          c=len(new_xy)
        else:
          c+=1
      c=0  

    # ===================================================
    # Filter invalid frames + convert frames into ints

    flame_int_list = [[int(float(j)) for j in i] for i in flame_frames]

    # ==========================
    # Dictionary Appendage

    if output == "CSV":
      for i in range(len(flame_split)):
        if(fixed_flame_locs[i] in global_dictionary): # If the location is already in the dictionary, add the next frames instead of completely replacing the old ones
          global_dictionary[fixed_flame_locs[i]] = global_dictionary.get(fixed_flame_locs[i]) + flame_int_list[i]
        else:
          global_dictionary[fixed_flame_locs[i]] = flame_int_list[i]

    if output == "DB":

      path_pattern = re.compile(r'/.+')
      for file in flame_files_only:
        flame = read_flames(file)
        f_pat = path_pattern.findall(flame)

        for loc in f_pat:
          flocation =  loc.split(" ")[1]
          fframe = loc.split(" ")[2:]

          c=0
          while(c<len(new_xy)):
            if("/" + flocation == f_xy[c]):
              temp = xyloc_prefix[c] + flocation
              break
            else:
              c+=1

          interval_fframe = interval_frames(fframe)

          str_frames = ' '.join((interval_fframe))
          if file in secondary_dict:
            secondary_dict[file].append(f'{temp} {str_frames}')
          else:
            secondary_dict[file] = [f'{temp} {str_frames}']
    
  # =====================================
  # Dictionary Frame Sort and Intervals (THIS SHOULD BE EXECUTED NO MATTER WHAT)
  for i, (loc, fram) in enumerate(global_dictionary.items()):
    # Only look at values on the first line with locations and frames
    if(i>=4):           
      # Eliminate all duplicate frames              
      non_dupes = list(set(fram))
      global_dictionary[loc] = interval_frames(non_dupes)

# ========================================
# EXPORTING OPTIONS (CSV OR DB)
# ========================================
# ======================
# CSV Export Option
# ======================

if output == 'CSV':
  with open('framefix.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Producer", "Operator", "Job", "Notes"])
    writer.writerow([global_dictionary[prod_split[0]], global_dictionary[oper_split[0]], global_dictionary[job_split[0]], global_dictionary[notes_split[0]]])
    writer.writerow([])

    for i, (loc, fram) in enumerate(global_dictionary.items()):
      if(i>=4):                         # Only look at values on the first line with locations 
        for j in range(len(fram)):      # Keep track of every locations frame index
          writer.writerow([loc, fram[j]])

# ======================
# MongoDB Export Option
# ======================
# MongoDB Setup
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
user_submit = os.getlogin()
# Collections
type1 = mydb['file_details']
type2 = mydb['file_content']

if output == 'DB':
  # Parsing File Names
  strip_init_jobs = []

  strip_final_jobs = []
  strip_init_xy = []
  strip_final_xy = []

  def parse_job_names():
    for file in jobs:
      strip_init_jobs.append(file.replace(".txt", ""))
    for file in strip_init_jobs:
      strip_final_jobs.append(file.split("_"))

  def parse_xy_names():
    strip_init_xy.append(xy_job.replace(".txt", ""))
    strip_final_xy.append(strip_init_xy[0].split("_"))

  def curr_time():
      time = datetime.now()
      str_time = time.strftime("%m/%d/%Y")
      return str_time

  parse_job_names()
  parse_xy_names()

  # ===================
  # Type1 Dictionaries
  # ===================

  # Baselight and Flame Initialized at 3 maximum (Not always going to use all of them)
  dict_t1_bf = [
    { "script_runner": user_submit, "file_machine": None, "file_user": None, "file_date": None, "submit_date": None},
    { "script_runner": user_submit, "file_machine": None, "file_user": None, "file_date": None, "submit_date": None},
    { "script_runner": user_submit, "file_machine": None, "file_user": None, "file_date": None, "submit_date": None}
  ]

  # Baselight and/or Flames
  for i in range(len(strip_final_jobs)):
    
    dict_t1_bf[i]['file_machine'] = strip_final_jobs[i][0]   # Machine
    dict_t1_bf[i]['file_user'] = strip_final_jobs[i][1]      # File Username
    
    timez = datetime.strptime(str(strip_final_jobs[i][2]), '%Y%m%d')
    temp_time = timez.strftime('%#m/%d/%Y')
    dict_t1_bf[i]['file_date'] = temp_time                   # File Date
    dict_t1_bf[i]['submit_date'] = curr_time()               # Submit/Current Time

  # ===================
  # Type2 Dictionaries
  # ===================
  dict_init_base = {}
  # Initial Baselight
  for i in range(len(baselight_int_list)):
    if(fixed_locs[i] in dict_init_base): # If the location is already in the dictionary, add the next frames instead of completely replacing the old ones
      dict_init_base[fixed_locs[i]] = dict_init_base.get(fixed_locs[i]) + interval_frames(baselight_int_list[i])
    else:   # Otherwise, add it the normal "replacing way"
      dict_init_base[fixed_locs[i]] = interval_frames(baselight_int_list[i])

  # Baselight and Flame Initialized at 3 maximum (Not always going to use all of them)
  dict_t2_base = [
    { "file_user": None, "file_date": None, "Frames_to_fix": None},
    { "file_user": None, "file_date": None, "Frames_to_fix": None},
    { "file_user": None, "file_date": None, "Frames_to_fix": None}
  ]

  # Baselight and Flame File Details
  for i in range(len(strip_final_jobs)):
    
    timez = datetime.strptime(str(strip_final_jobs[i][2]), '%Y%m%d')
    temp_time = timez.strftime('%#m/%d/%Y')
    dict_t2_base[i]['file_user'] = strip_final_jobs[i][1]
    dict_t2_base[i]['file_date'] = temp_time

  # Baselight Only Locations and Frames
  for user, (path, frames) in enumerate(dict_init_base.items()):
    for frame in frames:
      if not dict_t2_base[0]["Frames_to_fix"]:
        dict_t2_base[0]["Frames_to_fix"] = [f'{path} {frame}']
      else:
        dict_t2_base[0]["Frames_to_fix"].append(f'{path} {frame}')

  # Flame Only Frames
  for i, (file, content) in enumerate(secondary_dict.items()):
    # Skip the baselight index (always at 0)
    if i == 0:
        continue
    temp_frames = []
    for path_frames in content:
        # Splitting the frames should only happen ONCE
        path, frames = path_frames.split(' ', 1)
        for frame in frames.split():
            temp_frames.append(f'{path} {frame}')
    dict_t2_base[i]["Frames_to_fix"] = temp_frames
  
  # ===================
  # Insertions
  # ===================
  
  # Collection 1
  for i in range(len(args.workFiles)):
    type1.insert_one(dict_t1_bf[i])

  # Collection 2
  for i in range(len(args.workFiles)):
    type2.insert_one(dict_t2_base[i])

# ========================================
# DATA BASE CALLS OUTPUT
# ========================================

if args.verbose:
  # QUESTION 1: All of TDanza's Work
  print("\nQuestion 1: List all work done by user TDanza\n")
  danza = type2.find({'file_user': 'TDanza'})
  for d in danza:
    for job in d["Frames_to_fix"]:
      print(job)
      
  # QUESTION 2: All work done before 3-25-23 on a Flame
  print("\nQuestion 2: All work done before 3-25-2023 date on a Flame\n")
  f_files = []
  type1_grab = []

  for x in type1.find({"file_machine": 'Flame', "file_date": {'$lt': '3/25/2023'}}):
    type1_grab.append(x)

  for z in type1_grab:
    t1_users = z['file_user']
    t1_dates = z['file_date']
    for y in type2.find({"file_user": t1_users, "file_date": t1_dates}):
      f_files.append(y['Frames_to_fix'])
  for f in f_files:
    print(f)

  # QUESTION 3: What work was done on hpsans13 on 3-26-23
  print('\nQuestion 3: What work done on hpsans13 on date 3-26-2023\n')
  hp = type2.find({"file_date": '3/26/2023'})
  for h in hp:
    if h["Frames_to_fix"] == None:
      print("There is nothing!")
    else:
      for work in h["Frames_to_fix"]:
        if 'hpsans13' in work:
          print(work)

  # QUESTION 4: Names of all Flame users
  print("\nQuestion 4: Name of all Autodesk Flame users\n")
  auto_flame = type1.find({"file_machine": 'Flame'})
  flame_peeps = []
  for user in auto_flame:
    if user['file_user'] not in flame_peeps:
      print(user['file_user'])
      flame_peeps.append(user['file_user'])

# ========================================
# VIDEO PROCESSING AND JOB DATABASE CALLS
# ========================================

# If program was called just with a video
if args.vidFiles is not None:

  # Get maximum frame value for selected video with ffprobe
  video_file = args.vidFiles[0]
  command = f"ffprobe -v error -select_streams v:0 -show_entries stream=nb_frames -of default=nokey=1:noprint_wrappers=1 {video_file}"
  arguments = shlex.split(command)

  # Return number of frames
  result = subprocess.run(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, encoding="utf-8")
  nb_frames = int(result.stdout.strip())

  # ==========================================
  # Find all frame ranges within max frames
  # ==========================================

  # Remove all locations from DB calls entries while retaining nested list structure
  # Didn't really end up needing nested structure for these removers but too lazy to change them now lmao
  def struct_loc_remove(l):
    # If current entry is not a list
    if not isinstance(l, list):
      return re.sub(r'(/[^\s]+ )', '', l)
    return [struct_loc_remove(v) for v in l]
  
  # Remove all ranges from DB calls entries while retaining nested list structure
  def struct_range_remove(l):
    # If current entry is not a list
    if not isinstance(l, list):
        return re.sub(r'\b\d+(?:-\d+)?(?!\w)', '', l).strip()
    return [struct_range_remove(v) for v in l]

  # Convert frame range into a timecode
  def timecoder(l):
    temp = []
    for i in range(len(l)):
      hour = int(l[i]) / 216000
      minute = int(l[i]) / 3600 % 60
      second = (int(l[i]) % 3600) / 60
      frame = int(l[i]) % 3600 % 60
      time_code = ("%02d:%02d:%02d.%02d" % (hour, minute, second, frame))
      temp.append(time_code)
    return temp
  
  # Convert single frame into a timecode
  def single_timecoder(f):
    hour = int(f) / 216000
    minute = int(f) / 3600 % 60
    second = (int(f) % 3600) / 60
    frame = int(f) % 3600 % 60
    time_code = ("%02d:%02d:%02d.%02d" % (hour, minute, second, frame))
    return time_code

  print(f'All frame ranges within {nb_frames}:\n')
  # DB Call: Extract all 'Frames_to_fix' entries for each file
  all = type2.find({})
  loc_and_range = []

  for x in all:
    temp_range = x['Frames_to_fix']
    loc_and_range.append(temp_range)

  # Remove locations
  o_list = struct_loc_remove(loc_and_range)
  o_list2 = struct_range_remove(loc_and_range)

 # =====================================
 # === MIGHT NOT EVEN NEED STRUCTURE ===
 # =====================================
  # Iterate over number of different files
  for i in range(len(o_list)):
    # Then iterate over each frame and/or range in that file
    for j in range(len(o_list[i])):
        # Save the current frame and/or range and content as a temporary string
        num_range = o_list[i][j]

        # If it's a range:
        if '-' in num_range:
            # Remove '-' and split into two comparable ints
            num_range = num_range.split('-')
            # If at least one of them exceeds nb_frames, remove it
            if int(num_range[0]) > nb_frames or int(num_range[1]) > nb_frames:
                o_list[i][j] = None
                o_list2[i][j] = None

        # Otherwise, you can just remove 
        elif '-' not in num_range:
            o_list[i][j] = None
            o_list2[i][j] = None

  # Delete all 'None's using list comprehension
  filtered_list = [[x for x in sublist if x is not None] for sublist in o_list]
  filtered_list2 = [[x for x in sublist if x is not None] for sublist in o_list2]


  # ==========================
  # Create all upcoming lists
  # ==========================
  ranges = []
  locations = []

  timecodes = []
  timecodes_xls = []

  middle_frames = []
  thumbnail_locations = []

  # Eliminate file nested list structure
  # Also gets rid of any empty lists
  for file in filtered_list:
    for content in file:
      ranges.append(content)
      timecodes.append(content)

  for file in filtered_list2:
    for content in file:
      locations.append(content)

  # Convert ranges into structured nested timecode ranges
  # Also save middle-frames in a new list simultaneously for thumbnails later
  for i in range(len(timecodes)):
    current = ranges[i].split('-')

    subbed = int(current[1]) - int(current[0])
    # If there's only a single frame difference
    if subbed == 1:
      # Use the upper frame limit as the thumbnail
      middle_frames.append(single_timecoder(current[1]))
    else:
      # Divide the difference between the low and high limits of the range by 2
      # Also round down
      quo = subbed // 2
      # Subtract the quotient from the upper limit of the range
      mid = int(current[1]) - quo
      middle_frames.append(single_timecoder(str(mid)))

    timecodes[i] = (timecoder(current))
  print(middle_frames)
  # Create unstructured list of concatenated timecode strings for the xls
  for r in timecodes:
    t1 = r[0]
    t2 = r[1]
    newt = (t1 + ' - ' + t2)
    timecodes_xls.append(newt)

  # Get thumbnails
  for i in range(len(middle_frames)):
    # Extract one frame at a time with each middle_frame timecode
    # Thumbnail names increment by 1
    tb_command2 = f'ffmpeg -i {video_file} -ss {middle_frames[i]} -s 96x74 out{i}.png'
    arguments = shlex.split(tb_command2)
    process = subprocess.call(arguments)
    thumbnail_locations.append(f'out{i}.png')

  # ==========================================
  # XLS Exporting (List and Columns Setup)
  # ==========================================

  if output == 'XLS':
    # Create blank placeholder for thumbnail insertion
    # Just need this for the 4th column to be inserted
    placeholder = [None for _ in range(41)]
    df = pd.DataFrame(list(zip(locations, ranges, timecodes_xls, placeholder)),
                columns =['Location', 'Frame Ranges', 'Timecode Ranges', 'Thumbnails of Middle Frames'])
    df.to_excel('timecodes.xlsx', index=False)

    # ========================
    # Insert Thumbnails
    # ========================

    # Open the current timecodes file
    workbook = openpyxl.load_workbook('timecodes.xlsx')
    ws = workbook.active

    for i, thumb in enumerate(thumbnail_locations):
      img = Image(thumb)

      # Should start on the 2nd row and move up one by one
      # Always in 4th column 
      cell = ws.cell(row=i+2, column=4)

      img.width, img.height = 96, 74
      img.anchor = cell.coordinate

      ws.add_image(img)
    workbook.save('timecodes_with_thumbnails.xlsx')