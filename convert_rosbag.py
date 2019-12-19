import yaml
import argparse
import rosbag
from rospy_message_converter import message_converter
import numpy as np

YAML_DESC = "data_description"
YAML_DATATYPE = "datatype"
YAML_ALIAS = "name"
YAML_LENGTH = "length"

YAML_IDENT = "identifier"
IDENT_CONFIG = "c"
IDENT_DATA = "d"

#----------------------------------------------------------------------------------
class RosbagStructureParser:
  
  # extension for yaml-configuration template
  CONV_EXT = "_conv.yaml"
  
  YAML_HEADER = (  
  "# ----------------------------------------------------------------------\n" +
  "# - The section '{0}' defines the data identification symbols\n".format(YAML_IDENT) + 
  "# - The section '{0}' defines the data identification symbols\n".format(YAML_DESC) + 
  "# - Delete entries that should be ignored at the conversation from this document\n\n")
  
  YAML_IDENT_DESC = {YAML_IDENT:{
    IDENT_CONFIG: "Mark datafield as Configuration entry - should remain equal for all messages",
    IDENT_DATA: "Mark datafield as Data entry - should change for all messages"
    }}

  YAML_DATAFIELD_DESC = {YAML_DESC:{
    YAML_IDENT: "Identifier according to section '{0}'".format(YAML_IDENT),
    YAML_DATATYPE: "Identifier for !NUMPY STRUCTURED ARRAY! datatype!",
    YAML_ALIAS: "Unique (for this topic) Name that is used in the structured array for storing",
    YAML_LENGTH: "Optional, lenth for array elements"
    }}
    
  YAML_SEPERATOR = (
  "\n# ----------------------------------------------------------------------\n"
  "# ROS-Topics and their corresponding data-fields\n"
  )
  
  def __init__(self, bag_file, info_print, yaml_print):
    self.bag_file = bag_file
    self.info_print_ = info_print
    self.yaml_print_ = yaml_print
    
    self.yaml_file_ = self.bag_file[:-4] + self.CONV_EXT

  def parseRosbagStructure(self):
    bag = rosbag.Bag(self.bag_file)
    
    # get topics and ros-types of the rosbag
    topics = bag.get_type_and_topic_info()[1].keys()
    types = []
    for i in range(0,len(bag.get_type_and_topic_info()[1].values())):
      types.append(bag.get_type_and_topic_info()[1].values()[i][0])

    # add description entry to yaml
    dictionaries = {}
    # extract contents from first ros-message
    for i, topic in enumerate(topics):
      if self.info_print_: print(topic + ": " + types[i])
      
      for top, msg, t in bag.read_messages(topics=[topic]):
        dictionaries[topic] = message_converter.convert_ros_message_to_dictionary(msg)        
        self.addDatafieldSpecifiers_(dictionaries[topic])
        # get datatypes from first message
        break
    
    bag.close()
    
    # print enhanced structure to yaml-config file 
    if self.yaml_print_:
      with open(self.yaml_file_, 'w') as f:
        f.write(RosbagStructureParser.YAML_HEADER)
        f.write(yaml.dump(RosbagStructureParser.YAML_IDENT_DESC, default_flow_style=False))
        f.write(yaml.dump(RosbagStructureParser.YAML_DATAFIELD_DESC, default_flow_style=False))
        f.write(RosbagStructureParser.YAML_SEPERATOR)
        f.write(yaml.dump(dictionaries))
        print("\nThe following Yaml-template was created: " + self.yaml_file_)

  # Adds identifier entries to each leaf of the dictionary-tree
  def addDatafieldSpecifiers_(self, dictionary):
    for key in dictionary:
      
      # nested dictionary
      if isinstance(dictionary[key], dict):
        self.addDatafieldSpecifiers_(dictionary[key])
      
      # create description for each leaf according to its type 
      else:
        new_content = {YAML_IDENT: None, YAML_ALIAS: str(key)}
        
        # nested array
        if isinstance(dictionary[key], list):
          # empty lists are skipped
          if len(dictionary[key]) > 1:
            new_content[YAML_DATATYPE] = type(dictionary[key][0]).__name__
            new_content[YAML_LENGTH] = len(dictionary[key])
          else:
            print("WARNING: Skipped empty list element: " + str(key))
        else:
          new_content[YAML_DATATYPE] = type(dictionary[key]).__name__ 
          new_content[YAML_LENGTH] = 1
          
        dictionary[key] = new_content
        
  def getYamlFile(self):
    return self.yaml_file_

#----------------------------------------------------------------------------------
class Rosbag2DataConverter:

  def __init__(self, bag_file, yaml_config):
    
    self.YAML_PATH = 'path'    
    self.bag_ = rosbag.Bag(bag_file)
    
    # parse config-yaml
    with open(yaml_config, "r") as f:
      self.structure_ = yaml.load(f)
    
    # extract and then remove description elements 
    self.identifier_ = self.structure_[YAML_IDENT]
    self.datafields_ = sorted(self.structure_[YAML_DESC].keys())
      
    del self.structure_[YAML_IDENT]
    del self.structure_[YAML_DESC]
    
    # for each topic:
    self.field_entries_ = {} # data entries
    # storage structure for configuration and data
    self.dt_ = {IDENT_CONFIG:{}, IDENT_DATA:{}}
    
    for topic in self.structure_:
      field_entry = self.getDictPaths_(self.structure_[topic], [], {})
      self.field_entries_[topic] = field_entry
      
      # create storage structure definition
      for name in field_entry:        
        # point reference pointer to the right list -> operations are the same 
        cur_dt = self.dt_[field_entry[name][YAML_IDENT]] 
        # no according entry for this list yet          
        if topic not in cur_dt:
          cur_dt[topic] = []
          
        array_type = "({0}, {1})".format(self.bag_.get_message_count(topic), field_entry[name][YAML_LENGTH])
        cur_dt[topic].append((name, field_entry[name][YAML_DATATYPE], array_type))
    
    print(yaml.dump(self.field_entries_))
    print(yaml.dump(self.dt_))

    # TODO: create storage structure
    
    # save the data from bagfile to 2D array structure
    for topic in self.field_entries_:
      for top, msg, t in self.bag_.read_messages(topics=[topic]):
        for name in self.field_entries_[topic]:
          
          cur_field = self.field_entries_[topic][name]
          
          # get nested element according to data path in path dict
          element = message_converter.convert_ros_message_to_dictionary(msg)
          for nest in cur_field[self.YAML_PATH]:
            element = element[nest]
          
          # configuration datafield
          if cur_field[YAML_IDENT] == IDENT_CONFIG:
            # store once, and check if it stays the same
            pass
          
          elif cur_field[YAML_IDENT] == IDENT_DATA:
            # store for each iteration
            pass
          
          else:
            # shouldn't happen...
            print("A wild ERROR appeared! for: " + self.path2Str_([topic] + cur_field[self.YAML_PATH]))
            print("(1) FIGHT, (2) PROG, (3) ITEM, (4) RUN")
        # temp
        break
        
    self.bag_.close()  

  # each topic contains a list of paths for the corresponding datafield
  # each list element contains a dictionary defining the field-properties
  def getDictPaths_(self, dictionary, path, entries):
    for key in dictionary:
      
      # nested dictionary
      if isinstance(dictionary[key], dict):
              
        # dictionary contains a data-field description
        if sorted(dictionary[key].keys()) == self.datafields_:
          
          # copy and then adapt current data-description for later processing
          new_dict = dictionary[key].copy()
          
          # name is unique for later column reference
          if new_dict[YAML_ALIAS] not in entries:
            name = new_dict[YAML_ALIAS]
            del new_dict[YAML_ALIAS]
            new_dict[self.YAML_PATH] = path + [key]
            entries[name] = new_dict
          else:
            print("ERROR: Multiple entries for datafield: " + str(dictionary[key][YAML_ALIAS]))
        else:
          self.getDictPaths_(dictionary[key], path + [key], entries)
      
      # Potential error
      elif key in self.datafields_:
        print("WARNING: No full set or extra data-field properties for: " + self.path2Str_(path + [key]))        
      
      # invalid identifier
      else:
        print("ERROR: Invalid identifier used for: " + self.path2Str_(path + [key]))
        
    return entries
   
  def path2Str_(self, path):
    ret = ""
    for element in path:
        ret = ret + str(element) + "/"
    return ret

#----------------------------------------------------------------------------------
# script entry point
#

# command line argument parsing configuration
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--info', action='store_true', help="Print rosbag-topics and their types to console")
parser.add_argument('-f', '--file', help="Input rosbag", required=True)

# either the creation of the configuration template or the output data file can be chosen
group = parser.add_mutually_exclusive_group()
group.add_argument('-o', '--output', action='store_true', help="Create output-data file")
group.add_argument('-c', '--config', action='store_true', help="Create configuration-template (yaml) for later parsing")
  
args = parser.parse_args()

rsp = RosbagStructureParser(args.file, args.info, args.config)

if args.info or args.config:
  rsp.parseRosbagStructure()
  
if args.output:
  r2d = Rosbag2DataConverter(args.file, rsp.getYamlFile())

