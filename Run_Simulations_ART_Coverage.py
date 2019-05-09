#########################
## import dependencies ##
#########################

import json
import os
import time

import pandas

from dtk.utils.analyzers.DownloadAnalyzerTPI import DownloadAnalyzerTPI
from dtk.utils.builders.ConfigTemplate import ConfigTemplate
from dtk.utils.builders.TaggedTemplate import CampaignTemplate, DemographicsTemplate
from dtk.utils.builders.TemplateHelper import TemplateHelper
from dtk.utils.core.DTKConfigBuilder import DTKConfigBuilder
from simtools.AnalyzeManager.AnalyzeManager import AnalyzeManager
from simtools.ExperimentManager.ExperimentManagerFactory import ExperimentManagerFactory
from simtools.ModBuilder import ModBuilder
from simtools.SetupParser import SetupParser
from simtools.Utilities.Matlab import read_mat_points_file
from simtools.Utilities.COMPSUtilities import create_suite
from ast import literal_eval


#################
## set options ##
#################

JUST_TESTING = False

SetupParser.default_block = 'HPC'
tpi_csv_filename = "Nyanza_30_40_50_iter174.csv"
suite_name = 'Nyanza Base Case'

unused_params = [
    'TPI',
    'Male To Female Young',
    'Male To Female Old',
    'Risk Assortivity'
]

#####################################################################################
## map short parameter names to full paths to parameters in EMOD input files.      ##
## ensure that all parameters from Calibrated_Parameter_Column_Names.csv are here. ##
#####################################################################################


parameter_map = {
'Start_Year_Seeding_Year':'Start_Year__KP_Seeding_Year',
'Base_Infectivity':'Base_Infectivity',
'Circumcision_Reduced_Acquire':'Circumcision_Reduced_Acquire',
'Sexual_Debut_Age_Female_Weibull_Heterogeneity':'Sexual_Debut_Age_Female_Weibull_Heterogeneity',
'Sexual_Debut_Age_Female_Weibull_Scale':'Sexual_Debut_Age_Female_Weibull_Scale',
'Sexual_Debut_Age_Male_Weibull_Heterogeneity':'Sexual_Debut_Age_Male_Weibull_Heterogeneity',
'Sexual_Debut_Age_Male_Weibull_Scale':'Sexual_Debut_Age_Male_Weibull_Scale',
'TRANSITORY_Formation_Rate_Constant':'Society__KP_Defaults.TRANSITORY.Pair_Formation_Parameters.Formation_Rate_Constant',
'INFORMAL_Formation_Rate_Constant':'Society__KP_Defaults.INFORMAL.Pair_Formation_Parameters.Formation_Rate_Constant',
'MARITAL_Formation_Rate_Constant':'Society__KP_Defaults.MARITAL.Pair_Formation_Parameters.Formation_Rate_Constant',
'Male_To_Female_Relative_Infectivity_Multipliers': 'Male_To_Female_Relative_Infectivity_Multipliers',
'Defaults_TRANSITORY_Condom_Usage_Probability_Max':'Society__KP_Defaults.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Max',
'Defaults_TRANSITORY_Condom_Usage_Probability_Mid':'Society__KP_Defaults.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Mid',
'Defaults_TRANSITORY_Condom_Usage_Probability_Rate':'Society__KP_Defaults.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Rate',
'Defaults_INFORMAL_Condom_Usage_Probability_Max':'Society__KP_Defaults.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Defaults_INFORMAL_Condom_Usage_Probability_Mid':'Society__KP_Defaults.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Mid',
'Defaults_INFORMAL_Condom_Usage_Probability_Rate':'Society__KP_Defaults.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Rate',
'Defaults_MARITAL_Condom_Usage_Probability_Max':'Society__KP_Defaults.MARITAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Defaults_MARITAL_Condom_Usage_Probability_Mid':'Society__KP_Defaults.MARITAL.Relationship_Parameters.Condom_Usage_Probability.Mid',
'Defaults_MARITAL_Condom_Usage_Probability_Rate':'Society__KP_Defaults.MARITAL.Relationship_Parameters.Condom_Usage_Probability.Rate',
'TRANSITORY_LOW_Prob_Extra_Relationship_Male':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.LOW.Prob_Extra_Relationship_Male',
'TRANSITORY_MEDIUM_Prob_Extra_Relationship_Male':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.MEDIUM.Prob_Extra_Relationship_Male',
'TRANSITORY_LOW_Prob_Extra_Relationship_Female':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.LOW.Prob_Extra_Relationship_Female',
'TRANSITORY_MEDIUM_Prob_Extra_Relationship_Female':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.MEDIUM.Prob_Extra_Relationship_Female',
'INFORMAL_LOW_Prob_Extra_Relationship_Male':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.LOW.Prob_Extra_Relationship_Male',
'INFORMAL_LOW_Prob_Extra_Relationship_Female':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.LOW.Prob_Extra_Relationship_Male',
'INFORMAL_MEDIUM_Prob_Extra_Relationship_Male':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.MEDIUM.Prob_Extra_Relationship_Male',
'INFORMAL_MEDIUM_Prob_Extra_Relationship_Female':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.MEDIUM.Prob_Extra_Relationship_Female',
'TRANSITORY_LOW_Max_Simultaneous_Relationships_Male':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.LOW.Max_Simultaneous_Relationships_Male',
'TRANSITORY_LOW_Max_Simultaneous_Relationships_Female':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.LOW.Max_Simultaneous_Relationships_Female',
'INFORMAL_LOW_Max_Simultaneous_Relationships_Male':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.LOW.Max_Simultaneous_Relationships_Male',
'INFORMAL_LOW_Max_Simultaneous_Relationships_Female':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.LOW.Max_Simultaneous_Relationships_Female',
'TRANSITORY_MEDIUM_Max_Simultaneous_Relationships_Male':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.MEDIUM.Max_Simultaneous_Relationships_Male',
'TRANSITORY_MEDIUM_Max_Simultaneous_Relationships_Female':'Society__KP_Defaults_All_Nodes.TRANSITORY.Concurrency_Parameters.MEDIUM.Max_Simultaneous_Relationships_Female',
'INFORMAL_MEDIUM_Max_Simultaneous_Relationships_Male':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.MEDIUM.Max_Simultaneous_Relationships_Male',
'INFORMAL_MEDIUM_Max_Simultaneous_Relationships_Female':'Society__KP_Defaults_All_Nodes.INFORMAL.Concurrency_Parameters.MEDIUM.Max_Simultaneous_Relationships_Female',
'MARITAL_MEDIUM_Max_Simultaneous_Relationships_Male':'Society__KP_Defaults_All_Nodes.MARITAL.Concurrency_Parameters.MEDIUM.Max_Simultaneous_Relationships_Male',
'MARITAL_MEDIUM_Max_Simultaneous_Relationships_Female':'Society__KP_Defaults_All_Nodes.MARITAL.Concurrency_Parameters.MEDIUM.Max_Simultaneous_Relationships_Female',
'INFORMAL_Duration_Weibull_Heterogeneity':'Society__KP_Defaults.INFORMAL.Relationship_Parameters.Duration_Weibull_Heterogeneity',
'Homa_Bay_TRANSITORY_Condom_Usage_Probability_Max':'Society__KP_Homa_Bay.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Max',
'Homa_Bay_INFORMAL_Condom_Usage_Probability_Max': 'Society__KP_Homa_Bay.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Initial_Distribution_Risk_Homa_Bay':'Initial_Distribution__KP_Risk_Homa_Bay',
'Kisii_TRANSITORY_Condom_Usage_Probability_Max':'Society__KP_Kisii.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Max',
'Kisii_INFORMAL_Condom_Usage_Probability_Max':'Society__KP_Kisii.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Initial_Distribution_Risk_Kisii':'Initial_Distribution__KP_Risk_Kisii',
'Kisumu_TRANSITORY_Condom_Usage_Probability_Max':'Society__KP_Kisumu.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Max',
'Kisumu_INFORMAL_Condom_Usage_Probability_Max':'Society__KP_Kisumu.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Initial_Distribution_Risk_Kisumu':'Initial_Distribution__KP_Risk_Kisumu',
'Migori_TRANSITORY_Condom_Usage_Probability_Max':'Society__KP_Migori.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Max',
'Migori_INFORMAL_Condom_Usage_Probability_Max':'Society__KP_Migori.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Initial_Distribution_Risk_Migori':'Initial_Distribution__KP_Risk_Migori',
'Nyamira_TRANSITORY_Condom_Usage_Probability_Max':'Society__KP_Nyamira.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Max',
'Nyamira_INFORMAL_Condom_Usage_Probability_Max':'Society__KP_Nyamira.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Initial_Distribution_Risk_Nyamira':'Initial_Distribution__KP_Risk_Nyamira',
'Siaya_TRANSITORY_Condom_Usage_Probability_Max':'Society__KP_Siaya.TRANSITORY.Relationship_Parameters.Condom_Usage_Probability.Max',
'Siaya_INFORMAL_Condom_Usage_Probability_Max':'Society__KP_Siaya.INFORMAL.Relationship_Parameters.Condom_Usage_Probability.Max',
'Initial_Distribution_Risk_Siaya':'Initial_Distribution__KP_Risk_Siaya',
'Weighting_Matrix_RowMale_ColumnFemale_RiskAssortivity' : 'Weighting_Matrix_RowMale_ColumnFemale__KP_RiskAssortivity',
'PreART_Link_Ramp_Min':'Actual_IndividualIntervention_Config__KP_PreART_Link.Ramp_Min',
'PreART_Link_Ramp_Max':'Actual_IndividualIntervention_Config__KP_PreART_Link.Ramp_Max',
'PreART_Link_Ramp_MidYear':'Actual_IndividualIntervention_Config__KP_PreART_Link.Ramp_MidYear',
'ART_Link_Ramp_Max':'Actual_IndividualIntervention_Config__KP_ART_Link.Ramp_Max',
'ART_Link_Ramp_MidYear':'Actual_IndividualIntervention_Config__KP_ART_Link.Ramp_MidYear'
}

#################
##   methods   ##
#################

def find_and_eval(item):
    if isinstance(item, list):
        for index in range(len(item)):
            check_recurse(item, index)
    elif isinstance(item, dict):
        keys = item.keys()
        for key in keys:
            check_recurse(item, key)
    else:
        # do nothing
        pass

def check_recurse(item, indexer):
    if isinstance(item[indexer], str):
        if '[' in item[indexer]:
            item[indexer] = eval(item[indexer])
    else:
        find_and_eval(item[indexer])

def header_table_to_dict(points_df, index_name=None):
    # do column renaming to model parameter names
    header = list(points_df.columns)
    # for index in range(len(header)):
    #     item = header[index]
    #     header[index] = parameter_map.get(item, header[index])

    new_header = {k: parameter_map.get(k, k) for k in header}

    df = points_df.rename(columns=new_header)
    # df = pandas.DataFrame(data=table, columns=header)

    # initial distribution parameters need to be triplet values: [v, 1-v, 0], for v as the table input value
    for param in df.columns:
        # e.g. ['Homa_Bay', 'Kisii', 'Kisumu', 'Migori', 'Nyamira', 'Siaya'] initial distributions
        if 'Initial_Distribution__KP_Risk' in param: # e.g. 'Initial_Distribution__KP_Risk_Homa_Bay'
            new_values = pandas.Series(list( zip(df[param], 1 - df[param], pandas.Series([0]*len(df.index))) ))
            df.loc[:, param] = new_values
       # if 'Initial_Distribution__KP_no_circumcision' in param:
           # new_values = pandas.Series(list( zip(df[param], 1 - df[param])) )
           # df.loc[:, param] = new_values
        if 'Male_To_Female_Relative_Infectivity_Multipliers' in param:
            new_values = pandas.Series(list( zip(df[param], df[param], df[param]) ))
            df.loc[:, param] = new_values

    # Drop unused columns
    for unused in unused_params:
        if unused in df.columns: df.drop(unused, 1, inplace=True)
    if index_name:
        df[index_name] = df.index

    reloaded_dict = json.loads(pandas.io.json.dumps(df.to_dict(orient='records')))
    find_and_eval(reloaded_dict)
    return reloaded_dict


def read_csv_points_file(csv_filename):
    df = pandas.read_csv(csv_filename)
    # header = list(df.columns)
    # data = df.as_matrix()
    return df # header, data

    
    
##########################################
##  paths to EMOD input file templates  ##
##########################################
    
    
# Create the base path
current_dir = os.path.dirname(os.path.realpath(__file__))
plugin_files_dir = os.path.join(current_dir, 'InputFiles/Templates')
static_files_dir = os.path.join(current_dir, 'InputFiles/Static')

# Load the base config file
config = ConfigTemplate.from_file(os.path.join(plugin_files_dir, 'config.json'))
config.set_param("Enable_Demographics_Builtin", 0, allow_new_parameters=True)

# Load the campaigns
cpnFT = CampaignTemplate.from_file(os.path.join(plugin_files_dir, 'campaign_western_Kenya.json'))
cpnSQ = CampaignTemplate.from_file(os.path.join(plugin_files_dir, 'campaign_western_Kenya.json'))
campaigns = {"cpnFT":cpnFT, "cpnSQ":cpnSQ}

###### If your scenarios require choosing among multiple campaign files, load additional files and assign variable names to them: ######
# cpn2 = CampaignTemplate.from_file(os.path.join(plugin_files_dir, 'Campaigns', 'campaign_file_name_2.json'))
# cpn3 = CampaignTemplate.from_file(os.path.join(plugin_files_dir, 'Campaigns', 'campaign_file_name_2.json'))
# campaigns = {"cpn1":cpn1, "cpn2":cpn2, "cpn3":cpn3}

# Load the demographics
demog = DemographicsTemplate.from_file( os.path.join(static_files_dir, 'Demographics.json'))
demog_pfa = DemographicsTemplate.from_file( os.path.join(plugin_files_dir,'PFA_Overlay.json'))
demog_acc = DemographicsTemplate.from_file( os.path.join(plugin_files_dir,'Accessibility_and_Risk_IP_Overlay.json'))
demog_asrt = DemographicsTemplate.from_file( os.path.join(plugin_files_dir,'Risk_Assortivity_Overlay.json'))

##########################################
##  name and configure the scenarios    ##
##########################################
   
# Load the scenarios
scenario_header = [
    'Start_Year__KP_PrEP_HIGH_Start_Year',
    'Event_Coordinator_Config__KP_PrEP_HIGH_Event.Target_Gender',
    'Event_Coordinator_Config__KP_PrEP_HIGH_Event.Target_Age_Min',
    'Event_Coordinator_Config__KP_PrEP_HIGH_Event.Target_Age_Max',
    'Waning_Config__KP_PrEP_HIGH_Waning.Durability_Map.Times[1]',
    'Waning_Config__KP_PrEP_HIGH_Waning.Initial_Effect',
    'Start_Year__KP_PrEP_MED_Start_Year',
    'Event_Coordinator_Config__KP_PrEP_MED_Event.Target_Gender',
    'Event_Coordinator_Config__KP_PrEP_MED_Event.Target_Age_Min',
    'Event_Coordinator_Config__KP_PrEP_MED_Event.Target_Age_Max',
    'Waning_Config__KP_PrEP_MED_Waning.Durability_Map.Times[1]',
    'Waning_Config__KP_PrEP_MED_Waning.Initial_Effect',
    'Scenario',
    'Campaign_Template'
]

scenarios = [
    [2018, 'Female', 15, 49, 180, 0.0,  2018, 'Female', 15, 49, 180, 0.0,  'Baseline',        "cpnSQ"]
    ## [2018, 'Female', 15, 49, 180, 0.73, 2018, 'Female', 15, 49, 180, 0.0,  'High@40Age15-49', "cpnSQ"],
    ## [2018, 'Female', 15, 49, 180, 0.0,  2018, 'Female', 15, 49, 180, 0.73, 'Med@40Age15-49',  "cpnSQ"]
]


# And the points
# point_header, points = read_mat_points_file(tpi_matlab_filename)
# point_header, points = read_csv_points_file(tpi_csv_filename)
points_df = read_csv_points_file(tpi_csv_filename)

# We only take the first 3 points. Comment the following line to run the whole 250 points
if JUST_TESTING:
    points_df = points_df[0:2]

# Create the default config builder
config_builder = DTKConfigBuilder()

# Set which executable we want to use for the experiments in the script
config_builder.set_experiment_executable('bin/20181022_EMOD_HIV_binary_with_relative_risk_factor.exe')

# This is REQUIRED by the templates
config_builder.ignore_missing = True

# Get the dicts
points_dict = header_table_to_dict(points_df, index_name='TPI')
for point in points_dict:
    tpi = point.pop('TPI')
    if 'TAGS' not in point:
        point['TAGS'] = {}

    point['TAGS']['TPI'] = tpi

with open('points_dict.json', 'w') as fp:
    json.dump(points_dict, fp)

scenarios_df = pandas.DataFrame(scenarios, columns=scenario_header)
scenarios_dict = header_table_to_dict(scenarios_df)

with open('scenarios_dict.json', 'w') as fp:
    json.dump(scenarios_dict, fp)

if __name__ == "__main__":
    SetupParser.init()

    experiments = []      # All the experiment managers for all experiments
    experiments_ids = []  # Ids of the created experiments for resuming capabilities

    # Check if we want to resume
    if os.path.exists('ids.json'):
        print("Previous run detected... Run [N]ew, [R]esume, [A]bort?")
        resp = ""
        while resp not in ('N', 'R', 'A'):
            resp = input()
        if resp == "A":
            exit()
        elif resp == "R":
            # In the resume case, retrieve the ids and create the managers
            experiments_ids = json.load(open('ids.json', 'r'))
            for id in experiments_ids:
                experiments.append(ExperimentManagerFactory.from_experiment(str(id)))
        elif resp == "N":
            # Delete shelve file
            if os.path.exists('DownloadAnalyzerTPI.shelf'): os.remove('DownloadAnalyzerTPI.shelf')
            # Delete the ids
            os.remove('ids.json')

    # If experiment_ids is empty -> we need to commission
    if not experiments_ids:
        # Create a suite to hold all the experiments
        suite_id = create_suite(suite_name)

        # Create the scenarios
        for scenario in scenarios_dict:
            scenario_name = scenario['Scenario']
            campaign_tpl = campaigns[scenario.pop('Campaign_Template')]

            # For each scenario, combine with the points first
            combined = []
            for point in points_dict:
                current = {}
                current.update(scenario)
                current.update(point)
                combined.append(current)

            # Extract the headers
            headers = [k.replace('CONFIG.', '').replace('DEMOGRAPHICS.', '').replace('CAMPAIGN.', '') for k in combined[0].keys()]

            # Construct the table
            table = [list(c.values()) for c in combined]

            # Change some things in the config.json
            config.set_param('Config_Name', scenario_name)

            # Initialize the template
            tpl = TemplateHelper()
            tpl.set_dynamic_header_table(headers, table)
            tpl.active_templates = [config, campaign_tpl, demog, demog_pfa, demog_asrt, demog_acc]

            # Create an experiment builder
            experiment_builder = ModBuilder.from_combos(tpl.get_modifier_functions())
            experiment_manager = ExperimentManagerFactory.from_cb(config_builder)
            COMPS_experiment_name = scenario_name
           # COMPS_experiment_name = suite_name # I want hover-over in COMPS to be the suite name

            experiment_manager.run_simulations(exp_name=COMPS_experiment_name, exp_builder=experiment_builder, suite_id=suite_id)
            experiments.append(experiment_manager)
            experiments_ids.append(experiment_manager.experiment.exp_id)

        # Dump the experiment ids for resume
        with open('ids.json', 'w') as out:
            json.dump(experiments_ids, out)
			
    # Every experiments are created at this point -> Analyze
    am = AnalyzeManager(verbose=False, create_dir_map=False)
    for em in experiments:
        am.add_experiment(em.experiment)
#    am.add_analyzer(DownloadAnalyzerTPI(['output\\DemographicsSummary.json', 'config.json', 'output\\ReportHIVART.csv', 'output\\ReportHIVByAgeAndGender.csv'],
#                                        output_dir='Test HIV 1'))
    am.add_analyzer(DownloadAnalyzerTPI(['output\\ReportHIVByAgeAndGender.csv'], output_dir='Nyanza Base Case'))

    # While the experiments are running, we are analyzing every 15 seconds
    while not all([em.finished() for em in experiments]):
        map(lambda e: e.refresh_experiment(), experiments)
        print("Analyzing !")
        am.analyze()
        print("Waiting 15 seconds")
        time.sleep(15)

    # Analyze one last time when everything is complete
    am.analyze()
