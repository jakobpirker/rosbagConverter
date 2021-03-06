import yaml
import argparse
import rosbag
from rospy_message_converter import message_converter
import numpy as np
import pickle

YAML_DESC = "data_description"
YAML_DATATYPE = "datatype"
YAML_ALIAS = "name"
YAML_LENGTH = "length"

YAML_IDENT = "identifier"
IDENT_CONFIG = "c"
IDENT_DATA = "d"

# TODO: python datatypes to numpy datatypes conversion table?

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
        new_content = {YAML_IDENT: IDENT_DATA, YAML_ALIAS: str(key)}
        # TODO: map string types  
        # nested array
        if isinstance(dictionary[key], list):
          # empty lists are skipped
          if len(dictionary[key]) > 1:
            new_content[YAML_DATATYPE] = type(dictionary[key][0]).__name__
            new_content[YAML_LENGTH] = len(dictionary[key])
          else:
            new_content[YAML_DATATYPE] = None
            new_content[YAML_LENGTH] = None
            print("WARNING: Empty list element: " + str(key))
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
    self.PICKLE_EXT_ = ".pickle"
    
    self.out_file_ = bag_file[:-4] + self.PICKLE_EXT_
       
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
    self.data_ = {}    
    
    # creat data-type definition
    for topic in self.structure_:
      field_entries = self.getDictPaths_(self.structure_[topic], [], {})
      self.field_entries_[topic] = field_entries
      self.data_[topic] = {}
      dt = {} # datatypes for config/data 
      
      # create storage structure definition 
      for name in field_entries:        
        entry =  field_entries[name]
        # no identifier entry for this topic yet
        if entry[YAML_IDENT] not in dt:
          dt[entry[YAML_IDENT]] = []
        # TODO: map string types 
        # nested array
        if entry[YAML_LENGTH] > 1:
          array_type = (entry[YAML_LENGTH], )
          dt[entry[YAML_IDENT]].append((name, entry[YAML_DATATYPE], array_type))
        else:
          dt[entry[YAML_IDENT]].append((name, entry[YAML_DATATYPE]))
      
      if IDENT_CONFIG in dt:
        self.data_[topic][IDENT_CONFIG] = np.zeros(1, dtype=dt[IDENT_CONFIG])
        
      if IDENT_DATA in dt:
        self.data_[topic][IDENT_DATA] = np.zeros(self.bag_.get_message_count(topic), dtype=dt[IDENT_DATA])
    
    print("Successfully parsed configuration file. Reading bag-file...") 
    
    # save the data from bagfile to 2D array structure
    for topic in self.field_entries_:
      print("Reading topic: " + str(topic))
      msg_count = 0
      for top, msg, t in self.bag_.read_messages(topics=[topic]):
        
        for name in self.field_entries_[topic]:
          
          entry = self.field_entries_[topic][name]
          data = self.data_[topic][entry[YAML_IDENT]][name]
          
          # get nested element according to data path in path dict
          new_d = message_converter.convert_ros_message_to_dictionary(msg)
          for nest in entry[self.YAML_PATH]:
            new_d = new_d[nest]
          
          # configuration datafield
          if entry[YAML_IDENT] == IDENT_CONFIG:
            # store once, and check if it stays the same
            if msg_count == 0:
              # store
              data[0] = new_d
            # QUICKFIX! TODO: proper comparison in case of an array -> numpy thing
            elif entry[YAML_LENGTH] == 1:
              if data[0] != new_d:
                print("ERROR: Configuration changed {0}/{1}: {2} to {3}".format(topic, str(name), str(data[0]), str(new_d)))
                # data[0] = new_d
            elif entry[YAML_LENGTH] > 1:
              for i  in range(len(data)):
                # TODO: fix?
                if data[i] != new_d[i]:
                  print("ERROR: Configuration changed {0}/{1}: {2} to {3}".format(topic, str(name), str(data[i]), str(new_d[i])))
            else:
              # shouldn't happen
              print("Wrong configuration properties in: " + str(topic))
          
          elif entry[YAML_IDENT] == IDENT_DATA:
            # store for each iteration
            data[msg_count] = new_d
          
          else:
            # shouldn't happen...
            print("A wild ERROR appeared! for: " + self.path2Str_([topic] + entry[self.YAML_PATH]))
            print("(1) FIGHT, (2) PROG, (3) ITEM, (4) RUN")
            
        msg_count = msg_count + 1
        
    self.bag_.close()
    
    print("Successfully read-in the bag-file. Saving...")
    
    with open(self.out_file_, 'wb') as f:
      pickle.dump(self.data_, f, protocol=2)
      print("Saved data to: " + self.out_file_)  

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
            print("ERROR: Multiple entries for datafield: " + self.path2Str_(path) + str(dictionary[key][YAML_ALIAS]))
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

