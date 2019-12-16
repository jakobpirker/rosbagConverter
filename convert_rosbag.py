import yaml
import argparse
import rosbag
from rospy_message_converter import message_converter

YAML_IDENT = "identifier"
SPEC_CONFIG = "c"
SPEC_DATA = "d"

#----------------------------------------------------------------------------------
class RosbagStructureParser:
  IND = "   "
  CONV_EXT = '_conv.yaml'
  
  YAML_HEADER = (  
  "# Identifier specification\n"
  "{3}:\n"
  "{0}- {1} # mark as configuration (equal for each step)\n"
  "{0}- {2} # mark as data (differing with each step)\n"
  "{0}# use ascending numbers to indicate the column position at the array\n"
  "{0}# simply delete unneeded entries from this document"
  "\n").format(IND, SPEC_CONFIG, SPEC_DATA, YAML_IDENT)

  def __init__(self, bag_file, info_print, yaml_print):
    self.bag_file = bag_file
    self.info_print_ = info_print
    self.yaml_print_ = yaml_print
    
    self.yaml_file_ = self.bag_file[:-4] + self.CONV_EXT

  def parseRosbagStructure(self):
    if self.yaml_print_: 
      self.f_yaml_ = open(self.yaml_file_, 'w')
      self.f_yaml_.write(self.YAML_HEADER)
    bag = rosbag.Bag(self.bag_file)

    # get topics and ros-types of the rosbag
    topics = bag.get_type_and_topic_info()[1].keys()
    types = []
    for i in range(0,len(bag.get_type_and_topic_info()[1].values())):
      types.append(bag.get_type_and_topic_info()[1].values()[i][0])

    # extract ros-message
    dictionaries = {}
    for i, topic in enumerate(topics):
      if self.yaml_print_: self.f_yaml_.write("\n" + topic + ": \n")
      if self.info_print_: print("\n" + topic + " (" + types[i] + "):")
      
      for top, msg, t in bag.read_messages(topics=[topic]):
        dictionaries[topic] = message_converter.convert_ros_message_to_dictionary(msg)        
        # get datatypes from first message
        self.unpackDict_(dictionaries[topic], 1)
        break
    print("\n")

    bag.close()
    if self.yaml_print_:
      print("The following Yaml-template was created: " + self.yaml_file_)
      self.f_yaml_.close()
    
  def getYamlFile(self):
    return self.yaml_file_
  
  def unpackDict_(self, dictionary, depth):
    for key in dictionary:    
        if self.yaml_print_:
          self.f_yaml_.write(self.IND*depth + str(key) + ": \n")
        if self.info_print_:
          print(self.IND*depth + str(key) + self.IND + str(type(dictionary[key]))[6:-1])
        if isinstance(dictionary[key], dict):
          self.unpackDict_(dictionary[key], depth + 1)

#----------------------------------------------------------------------------------
class Rosbag2DataConverter:

  def __init__(self, bag_file, yaml_config):
    self.yaml_config_ = yaml_config
    self.bag_file_ = bag_file
    
    # parse config-yaml
    self.structure_ = yaml.load(open(self.yaml_config_, "r"))
    self.identifier_ = self.structure_[YAML_IDENT]
    # delete identifier entry for later parsing
    del self.structure_[YAML_IDENT]
    
    self.data_paths_ = {}    

    # reorder topics
    for topic in self.structure_:
      self.cur_topic_ = topic
      # create a new entry for each topic, and sort content according to identifieres
      self.data_paths_[topic] = {}
      self.getDictPaths_(self.structure_[topic], [topic])
    
    print(yaml.dump(self.data_paths_))
        
    # read and save the data from bagfile
    bag = rosbag.Bag(self.bag_file_)
    for topic in self.data_paths_.keys():
      for top, msg, t in bag.read_messages(topics=[topic]):
        for key in self.data_paths_[topic]:
          # for the sake of shorter code: outer loop (could also be inside if-blocks)
          for path in self.data_paths_[topic][key]:
            # get nested element according to data path in path dict
            element = message_converter.convert_ros_message_to_dictionary(msg)
            for nest in path:
              element = element[nest]
            
            # use numpy structured array for storing!
            
            
            if isinstance(key, int):          
              # store in array according to its column number -> check numbers first?
              pass              
              
            elif key == SPEC_CONFIG:
              # save once and afterwards just
              # check if config parameter hasn't changed
              pass
              
            elif key == SPEC_DATA:
              # store in array
              pass
            
            else:
              # shouldn't happen...
              print("A wild ERROR appered! (1) FIGHT, (2) PROG, (3) ITEM, (4) RUN")
        # temp
        break

  def getDictPaths_(self, dictionary, path):
    for key in dictionary:
      # nested dictionary
      
      if isinstance(dictionary[key], dict):
        self.getDictPaths_(dictionary[key], path + [key])
      
      # no identifier specified
      elif dictionary[key] == None:
        print("WARNING: No identifier used for: " + self.path2Str_(path + [key]))
        
      # identifier entry already existing in current data_paths dict
      elif dictionary[key] in self.data_paths_[path[0]]:
        
        # column identifier already existing -> each number only allowed once!
        if isinstance(dictionary[key], int):
          print("ERROR: Multiple entries with same index, skipping new entry!")
          print("Existing: " + self.path2Str_([path[0]] + self.data_paths_[path[0]][dictionary[key]]))
          print("New: " + self.path2Str_(path + [key]))
        
        # type can simply be added
        else:
          self.data_paths_[path[0]][dictionary[key]].append(path[1:] + [key])
      
      # valid identifier, but not existing in current data_paths dict yet
      elif isinstance(dictionary[key], int) or (dictionary[key] in self.identifier_):
        self.data_paths_[path[0]][dictionary[key]] = []
        self.data_paths_[path[0]][dictionary[key]].append(path[1:] + [key])            
      
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
parser.add_argument('-i', '--info', action='store_true', help="Print info about rosbag to console")
parser.add_argument('-f', '--file', help="Input rosbag", required=True)

# either the creation of the configuration template or the output data file can be chosen
group = parser.add_mutually_exclusive_group()
group.add_argument('-o', '--output', action='store_true', help="Create output-data file")
group.add_argument('-c', '--config', action='store_true', help="Create configuration-template (yaml) for later parsing")
  
args = parser.parse_args()

rsp = RosbagStructureParser(args.file, args.info, args.config)

if args.info or args.config:
  rsp.parseRosbagStructure()
  
elif args.output:
  r2d = Rosbag2DataConverter(args.file, rsp.getYamlFile())
else:
  print("Whhooops... Sth went wrong...")

