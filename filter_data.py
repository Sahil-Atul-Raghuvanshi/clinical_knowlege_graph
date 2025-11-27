import pandas as pd
import os
import shutil
from pathlib import Path

def create_directory_structure():
    """Create the same directory structure in Filtered_Data as in Complete_Data"""
    base_path = Path("Filtered_Data")
    
    # Create main directories
    directories = ["hosp", "icu", "ed", "note"]
    for dir_name in directories:
        dir_path = base_path / dir_name
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {dir_path}")

def filter_csv_by_subject_ids_chunked(input_file, output_file, subject_ids, chunk_size=10000):
    """Filter a CSV file by multiple subject_ids using chunked reading for large files"""
    try:
        print(f"Processing: {input_file}")
        
        # Check if file exists and get its size
        if not os.path.exists(input_file):
            print(f"File not found: {input_file}")
            return False
            
        file_size = os.path.getsize(input_file) / (1024 * 1024)  # Size in MB
        print(f"File size: {file_size:.1f} MB")
        
        # Read first chunk to check columns
        first_chunk = pd.read_csv(input_file, nrows=1)
        
        # Check if subject_id column exists
        if 'subject_id' not in first_chunk.columns:
            print(f"No 'subject_id' column found in {input_file} - copying entire file")
            # Copy the entire file to the filtered folder
            shutil.copy2(input_file, output_file)
            print(f"[SUCCESS] Copied entire file {input_file} to {output_file}")
            return True
        
        # Process file in chunks
        filtered_rows = []
        total_rows = 0
        
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            total_rows += len(chunk)
            # Filter for all subject_ids in the list
            filtered_chunk = chunk[chunk['subject_id'].isin(subject_ids)]
            if len(filtered_chunk) > 0:
                filtered_rows.append(filtered_chunk)
            
            # Progress indicator for large files
            if total_rows % (chunk_size * 10) == 0:
                print(f"  Processed {total_rows:,} rows...")
        
        # Combine all filtered chunks
        if filtered_rows:
            filtered_df = pd.concat(filtered_rows, ignore_index=True)
            filtered_df.to_csv(output_file, index=False)
            print(f"[SUCCESS] Filtered {len(filtered_df)} rows from {total_rows:,} total rows in {input_file}")
        else:
            # Create empty file with headers
            first_chunk.iloc[0:0].to_csv(output_file, index=False)
            print(f"[SUCCESS] No matching rows found in {input_file} (processed {total_rows:,} rows)")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error processing {input_file}: {str(e)}")
        return False

def main():
    """Main function to filter all CSV files by multiple subject_ids"""
    # Define the list of subject_ids to filter
    # You can modify this list to include any subject_ids you want
    subject_ids = [10001401, 10010440, 10017886, 10028767, 10038332, 10067306, 10089894, 10099497, 10100027, 10137104, 10140296, 10144145, 10144424, 10160899, 10163695, 10196368, 10206502, 10215095, 10232096, 10277390, 10288895, 10308582, 10316237, 10330091, 10330431, 10337896, 10338515, 10348351, 10350765, 10356845, 10358580, 10364180, 10374990, 10382912, 10388400, 10404360, 10411588, 10427193, 10429620, 10434107, 10444484, 10455613, 10476891, 10500002, 10507925, 10509175, 10526776, 10538044, 10538657, 10541652, 10548962, 10558225, 10559181, 10569306, 10569882, 10575930, 10576063, 10581736, 10586112, 10599327, 10607290, 10610628, 10616548, 10627268, 10631298, 10643643, 10662430, 10667935, 10681957, 10701918, 10740871, 10742969, 10750124, 10775892, 10780367, 10805035, 10805746, 10818124, 10821855, 10822707, 10823188, 10832658, 10850680, 10852773, 10860211, 10862025, 10865559, 10867202, 10872780, 10873131, 10900387, 10908761, 10909653, 10923578, 10948636, 10970125, 10974073, 10976602, 10979912, 10980012, 10980593, 10986523, 10996711, 10997090, 11026758, 11036075, 11047741, 11059274, 11081047, 11084812, 11084839, 11093401, 11096116, 11098660, 11107102, 11109975, 11116402, 11130089, 11152968, 11153842, 11162709, 11169203, 11181210, 11189105, 11193726, 11198939, 11200955, 11208106, 11225343, 11236474, 11240073, 11258504, 11269805, 11271109, 11272213, 11273035, 11277677, 11282635, 11299992, 11309053, 11317535, 11325767, 11327015, 11345609, 11360506, 11365932, 11371820, 11378149, 11381569, 11385174, 11406241, 11413667, 11414906, 11418556, 11418995, 11423643, 11428852, 11429550, 11431483, 11435642, 11440245, 11443963, 11456728, 11458964, 11459376, 11473097, 11474372, 11485288, 11502644, 11510947, 11545574, 11546805, 11546816, 11569042, 11573149, 11577197, 11589725, 11599852, 11600106, 11604380, 11607482, 11611745, 11619091, 11648170, 11666315, 11673731, 11678433, 11687109, 11705029, 11708854, 11714071, 11725311, 11744037, 11757280, 11764279, 11788221, 11807843, 11818101, 11821055, 11824047, 11825462, 11833822, 11842519, 11844680, 11873746, 11885384, 11911069, 11921191, 11923449, 11924068, 11928413, 11928692, 11961264, 11964206, 11968605, 11976099, 12054856, 12067437, 12074041, 12080206, 12093095, 12107462, 12151259, 12151993, 12162956, 12166138, 12200502, 12203449, 12238614, 12250221, 12257167, 12261485, 12265465, 12266578, 12267107, 12279787, 12305092, 12350449, 12390274, 12406896, 12409853, 12414328, 12422071, 12438698, 12440965, 12465184, 12478986, 12480287, 12482552, 12501221, 12514289, 12527516, 12532356, 12547577, 12548159, 12557139, 12563258, 12566298, 12576502, 12582857, 12596118, 12606283, 12608786, 12612204, 12617635, 12634755, 12640507, 12650978, 12663841, 12663976, 12668827, 12670600, 12676048, 12679298, 12694545, 12724649, 12726133, 12730675, 12734486, 12736236, 12749568, 12749849, 12759187, 12763223, 12776202, 12776289, 12788516, 12796618, 12797228, 12799102, 12825445, 12832992, 12842440, 12847427, 12875556, 12882985, 12893355, 12905985, 12906270, 12911846, 12914859, 12936882, 12942909, 12968967, 12972832, 12975809, 12990990, 12996303, 13035922, 13057766, 13086918, 13109130, 13128765, 13137769, 13162579, 13170723, 13173167, 13213665, 13235049, 13253226, 13279128, 13279382, 13288965, 13294108, 13294123, 13299566, 13306067, 13312240, 13325402, 13358526, 13366982, 13368060, 13375144, 13377359, 13386304, 13386388, 13413853, 13442266, 13442831, 13447286, 13450581, 13495550, 13500916, 13512842, 13526588, 13536330, 13539061, 13568094, 13568606, 13572100, 13572190, 13576620, 13599443, 13603051, 13608577, 13620446, 13620449, 13621035, 13622750, 13622907, 13645451, 13684752, 13697582, 13736592, 13762865, 13764015, 13764116, 13766350, 13782765, 13785306, 13814783, 13818699, 13826513, 13839057, 13855588, 13858677, 13859242, 13875316, 13884765, 13889326, 13896515, 13999026, 14011743, 14014677, 14045504, 14045846, 14074579, 14082885, 14099038, 14124404, 14148228, 14151932, 14152034, 14155070, 14170029, 14175953, 14178725, 14179163, 14185804, 14189406, 14219343, 14222873, 14240547, 14242599, 14266489, 14281337, 14286235, 14300755, 14306537, 14318199, 14329220, 14331310, 14353044, 14357761, 14359057, 14381330, 14381706, 14396945, 14401469, 14418295, 14426687, 14440691, 14443175, 14446260, 14462115, 14473270, 14490420, 14497427, 14506968, 14510665, 14531429, 14538785, 14539850, 14548229, 14548428, 14549473, 14569190, 14582685, 14598157, 14600308, 14614127, 14616811, 14630468, 14642114, 14643398, 14677665, 14681017, 14689985, 14690648, 14695460, 14706408, 14731346, 14733192, 14744387, 14809300, 14825011, 14825995, 14834029, 14835486, 14847272, 14849693, 14851188, 14851197, 14875383, 14877188, 14878442, 14894374, 14896868, 14903260, 14925938, 14935657, 14936496, 14939413, 14940318, 14954698, 14960904, 14984479, 14988144, 14993854, 14996161, 15006963, 15012885, 15020369, 15023287, 15034585, 15040903, 15059385, 15063434, 15066909, 15084163, 15087570, 15123388, 15146844, 15149227, 15149925, 15150825, 15173422, 15178141, 15186614, 15191472, 15196539, 15200895, 15208360, 15221724, 15230030, 15250384, 15251751, 15259244, 15263884, 15264819, 15266132, 15304959, 15305253, 15307141, 15323393, 15325407, 15335912, 15345415, 15352109, 15355207, 15364266, 15387835, 15395941, 15406041, 15415643, 15421087, 15425725, 15426161, 15441487, 15443439, 15446959, 15451659, 15480743, 15499099, 15500935, 15557817, 15559171, 15559549, 15562207, 15573773, 15585879, 15590412, 15606926, 15616719, 15621159, 15621186, 15621580, 15622498, 15627285, 15646731, 15662090, 15700300, 15716281, 15722108, 15731508, 15738076, 15741521, 15745117, 15757588, 15758816, 15761754, 15775667, 15779915, 15785077, 15791443, 15798883, 15803890, 15816613, 15821500, 15835816, 15845632, 15848956, 15851934, 15856008, 15861255, 15875001, 15886636, 15904475, 15904892, 15912324, 15919557, 15919840, 15921116, 15942634, 15942934, 15950413, 15977504, 15985181, 15987101, 16013042, 16018443, 16022588, 16027364, 16033463, 16038130, 16043614, 16043625, 16047638, 16067831, 16086687, 16099802, 16130543, 16132649, 16133657, 16151261, 16159091, 16164016, 16171489, 16181355, 16199936, 16209157, 16211391, 16217957, 16218350, 16269879, 16271251, 16313531, 16316457, 16320691, 16323640, 16377954, 16386883, 16390325, 16395111, 16395494, 16413061, 16428221, 16434143, 16447390, 16454913, 16472097, 16505791, 16509298, 16510199, 16513544, 16536493, 16538283, 16538698, 16540289, 16540839, 16568313, 16568465, 16573341, 16573705, 16591395, 16614128, 16622129, 16627975, 16657295, 16666777, 16676410, 16693848, 16731219, 16736254, 16743102, 16744121, 16753239, 16809525, 16815103, 16828535, 16829440, 16843842, 16863257, 16873672, 16875792, 16879858, 16880672, 16888989, 16900658, 16902634, 16912219, 16912401, 16914410, 16921511, 16934035, 16940835, 16950228, 16965396, 16967171, 16969063, 16976054, 16986540, 16991358, 17019564, 17041034, 17055144, 17055995, 17071231, 17077020, 17094951, 17120028, 17121948, 17131757, 17131877, 17217543, 17222442, 17230816, 17232910, 17235944, 17237709, 17237928, 17247694, 17250375, 17264481, 17270380, 17285723, 17293606, 17296211, 17303590, 17317655, 17321845, 17329106, 17343455, 17353623, 17369878, 17372011, 17387103, 17389256, 17392100, 17399435, 17410025, 17424198, 17425991, 17452126, 17463554, 17485472, 17496927, 17497339, 17511937, 17516901, 17532555, 17539975, 17543562, 17545223, 17556194, 17559288, 17632059, 17635175, 17637680, 17638040, 17645807, 17668240, 17669276, 17672254, 17680002, 17683532, 17717605, 17725424, 17736294, 17799305, 17819706, 17837987, 17838494, 17843033, 17852478, 17856343, 17859488, 17863141, 17866544, 17882735, 17890530, 17897339, 17905563, 17916721, 17924392, 17944369, 17948154, 17953273, 17967763, 17968329, 18001923, 18011066, 18013971, 18033939, 18036964, 18056245, 18057037, 18066001, 18068147, 18077571, 18097264, 18119376, 18124225, 18128235, 18128277, 18131667, 18148236, 18148694, 18172623, 18187740, 18193043, 18193607, 18199819, 18201582, 18203271, 18211278, 18217695, 18218339, 18230892, 18253112, 18268241, 18284271, 18295542, 18315192, 18319079, 18326595, 18331587, 18344973, 18355815, 18370527, 18386668, 18400480, 18411232, 18414729, 18449910, 18454097, 18456328, 18460084, 18478557, 18546999, 18574969, 18583067, 18596671, 18615099, 18630754, 18649866, 18660215, 18691670, 18692227, 18696923, 18699338, 18703566, 18718699, 18737769, 18758678, 18760108, 18768021, 18797174, 18802058, 18804107, 18805037, 18809552, 18821140, 18823151, 18830695, 18837021, 18852216, 18902344, 18906987, 18915250, 18922322, 18935324, 18940898, 18946719, 18949692, 18949819, 18950662, 18954232, 18960171, 18960360, 18965721, 18978682, 18981283, 19019425, 19022227, 19065401, 19092032, 19105762, 19107550, 19110715, 19113397, 19131119, 19157078, 19214258, 19227105, 19250395, 19253639, 19260901, 19290303, 19299856, 19305674, 19352275, 19357104, 19361508, 19367944, 19381528, 19397112, 19405152, 19432635, 19438264, 19443779, 19495094, 19509298, 19512875, 19520448, 19523301, 19557342, 19561603, 19563570, 19581033, 19588353, 19593791, 19599279, 19616833, 19622090, 19626709, 19629076, 19629250, 19631111, 19650277, 19657931, 19659653, 19661729, 19669180, 19669877, 19674536, 19676805, 19682346, 19693912, 19694277, 19698713, 19700168, 19709832, 19713100, 19715857, 19724138, 19724694, 19735459, 19735607, 19738336, 19753816, 19769489, 19787228, 19792113, 19797689, 19797807, 19800337, 19832055, 19839466, 19840732, 19846721, 19855614, 19867017, 19880090, 19884099, 19910997, 19911351, 19918048, 19921471, 19934880, 19957809, 19973027]
    print(f"\n{'='*80}")
    print(f"FILTERING DATA FOR SUBJECT IDs: {subject_ids}")
    print(f"Source: Complete_Data/")
    print(f"Destination: Filtered_Data/")
    print(f"{'='*80}\n")
    
    # Create directory structure
    create_directory_structure()
    
    # Define CSV files used in the scripts (organized by directory)
    # Files NOT in this list will be skipped during filtering
    files_to_process = {
        "Complete_Data\\hosp": [
            # "admissions.csv",           # Scripts: 1_add_patient_nodes, 2_patient_flow, 10_add_provider_nodes
            # "transfers.csv",            # Scripts: 2_patient_flow
            # "services.csv",             # Scripts: 2_patient_flow
            # "prescriptions.csv",        # Scripts: 4_add_prescription_nodes
            # "microbiologyevents.csv",   # Scripts: 9_add_micro_biology_events
            # "drgcodes.csv",             # Scripts: 8_add_drg_codes
            # "labevents.csv",            # Scripts: 7_add_labevent_nodes
            # "d_labitems.csv",           # Scripts: 7_add_labevent_nodes (lookup table)
            # "diagnoses_icd.csv",        # Scripts: 6_add_diagnosis_nodes
            # "d_icd_diagnoses.csv",      # Scripts: 6_add_diagnosis_nodes (lookup table)
            # "procedures_icd.csv",       # Scripts: 5_add_procedure_nodes
            # "d_icd_procedures.csv",     # Scripts: 5_add_procedure_nodes (lookup table)
            # "patients.csv",             # Scripts: 1_add_patient_nodes
            # "omr.csv",                   # Scripts: 10_add_provider_nodes
        ],  
        "Complete_Data\\icu": [
            # "d_items.csv",              # Scripts: 5_add_procedure_nodes, 50_add_chart_events (lookup table)
            # "procedureevents.csv",      # Scripts: 5_add_procedure_nodes
            # "icustays.csv",             # Scripts: 3_add_icu_stays_label
            # "outputevents.csv"
            "chartevents.csv"
        ],
        "Complete_Data\\ed": [
            # "edstays.csv",              # Scripts: 2_patient_flow
            # "medrecon.csv",             # Scripts: 4_add_prescription_nodes
            # "pyxis.csv",                # Scripts: 4_add_prescription_nodes
            # "triage.csv",               # Scripts: 11_add_assessment_nodes
            # "diagnosis.csv",            # Scripts: 6_add_diagnosis_nodes
        ],
        "Complete_Data\\note": [
            # "discharge.csv",            # Scripts: 48_convert_text_clinical_node_to_json (input)
            # Note: discharge_clinical_note_json.csv and discharge_clinical_note_flattened.csv 
            # are generated files, not source files to be filtered
        ],
    }
    
    total_files_processed = 0
    total_files_successful = 0
    total_files_skipped = 0
    
    for source_dir, csv_files_to_filter in files_to_process.items():
        target_dir = source_dir.replace("Complete_Data", "Filtered_Data")
        
        print(f"\nProcessing directory: {source_dir}")
        print(f"-" * 80)
        
        # Check if source directory exists
        if not os.path.exists(source_dir):
            print(f"Source directory {source_dir} does not exist, skipping...")
            continue
        
        # Get all CSV files in the source directory
        all_csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
        
        if not all_csv_files:
            print(f"No CSV files found in {source_dir}")
            continue
        
        print(f"Found {len(all_csv_files)} total CSV files")
        print(f"Processing {len(csv_files_to_filter)} files used in scripts")
        
        # Report files that will be skipped
        skipped_files = [f for f in all_csv_files if f not in csv_files_to_filter]
        if skipped_files:
            print(f"Skipping {len(skipped_files)} unused files: {', '.join(skipped_files)}")
            total_files_skipped += len(skipped_files)
        
        for csv_file in csv_files_to_filter:
            input_path = os.path.join(source_dir, csv_file)
            output_path = os.path.join(target_dir, csv_file)
            
            # Check if file exists before processing
            if not os.path.exists(input_path):
                print(f"[WARNING] Expected file not found: {csv_file}")
                continue
            
            total_files_processed += 1
            
            if filter_csv_by_subject_ids_chunked(input_path, output_path, subject_ids):
                total_files_successful += 1
    
    print(f"\n{'='*80}")
    print(f"=== SUMMARY ===")
    print(f"{'='*80}")
    print(f"Subject IDs filtered: {subject_ids}")
    print(f"Total files processed: {total_files_processed}")
    print(f"Successfully processed: {total_files_successful}")
    print(f"Failed: {total_files_processed - total_files_successful}")
    print(f"Files skipped (not used in scripts): {total_files_skipped}")
    print(f"Data saved to Filtered_Data/ (filtered by subject_ids or copied entirely if no subject_id column)")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
