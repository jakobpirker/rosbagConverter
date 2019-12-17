import yaml
import argparse
import rosbag
from rospy_message_converter import message_converter

YAML_DESC = "data_description"
YAML_DATATYPE = "datatype"
YAML_ALIAS = "name"

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
    YAML_DATATYPE: ("Identifier for final storing, for {0} leave it as it is,".format(IDENT_CONFIG) +  
                    "for {0} use !NUMPY STRUCTURED ARRAY! datatype!".format(IDENT_DATA)),
    YAML_ALIAS: "Name that is used in the structured array for storing"
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
        self.unpackDict_(dictionaries[topic], 1)
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
    
  def getYamlFile(self):
    return self.yaml_file_
  
  def unpackDict_(self, dictionary, depth):
    for key in dictionary:
      
      # nested dictionary
      if isinstance(dictionary[key], dict):
        self.unpackDict_(dictionary[key], depth + 1)
      
      # create description for each leave according to its type 
      else:
        new_content = {YAML_IDENT: None, YAML_ALIAS: str(key)}
        
        if isinstance(dictionary[key], list):
          # TODO: handle correctly
          if len(dictionary[key]) > 1:
            new_content[YAML_DATATYPE] = type(dictionary[key][0]).__name__ + "[" + str(len(dictionary[key])) + "]"
          else:
            new_content[YAML_DATATYPE] = "empty list..."
        else:
          new_content[YAML_DATATYPE] = type(dictionary[key]).__name__ 
          
        dictionary[key] = new_content

#----------------------------------------------------------------------------------
class Rosbag2DataConverter:

  def __init__(self, bag_file, yaml_config):
    self.bag_file_ = bag_file
    
    # parse config-yaml
    with open(yaml_config, "r") as f:
      self.structure_ = yaml.load(f)
      
    self.identifier_ = self.structure_[YAML_IDENT]
    self.datafields_ = sorted(self.structure_[YAML_DESC].keys())
    
    # delete description entries for later parsing
    del self.structure_[YAML_IDENT]
    del self.structure_[YAML_DESC]
    
    self.data_paths_ = {}    

    # each topic contains a list of paths for the corresponding datafield
    # each list element contains a dictionary defining the field-properties
    self.getDictPaths_(self.structure_, [])
    
    # print(yaml.dump(self.data_paths_))
    
    # use numpy structured array for storing!
    # TODO: create data-structure
        
    # read and save the data from bagfile
    bag = rosbag.Bag(self.bag_file_)
    for topic in self.data_paths_.keys():
      for top, msg, t in bag.read_messages(topics=[topic]):
        for path in self.data_paths_[topic]:
          
          # last element contains data description
          dict_path = path[:-1]
          datafields = path[-1]
          
          # get nested element according to data path in path dict
          element = message_converter.convert_ros_message_to_dictionary(msg)
          for nest in dict_path:
            element = element[nest]
          
          # configuration datafield
          if datafields[YAML_IDENT] == IDENT_CONFIG:
            # store once, and check if it stays the same
            pass
          
          elif datafields[YAML_IDENT] == IDENT_DATA:
            # store for each iteration
            pass
          
          else:
            # shouldn't happen...
            print("A wild ERROR appeared! (1) FIGHT, (2) PROG, (3) ITEM, (4) RUN")
        # temp
        break

  def getDictPaths_(self, dictionary, path):
    for key in dictionary:
      
      # nested dictionary
      if isinstance(dictionary[key], dict):
              
        # dictionary contains a data-field description
        if sorted(dictionary[key].keys()) == self.datafields_:
        
          # key for current topic not yet in data_paths_
          if path[0] not in self.data_paths_:
            self.data_paths_[path[0]] = []
           
          self.data_paths_[path[0]].append(path[1:] + [key] + [dictionary[key]])
          
        else:
          self.getDictPaths_(dictionary[key], path + [key])
      # Potential error
      elif key in self.datafields_:
        print("WARNING: No full set of data-field properties for: " + self.path2Str_(path + [key]))        
      
      # invalid identifier
      else:
        print("ERROR: Invalid identifier used for: " + self.path2Str_(path + [key]))
    
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


