#!python3

import os
import sys
import time
import logging

this_file_dir = os.path.dirname(os.path.abspath(__file__))
method_local_dir = os.path.join(this_file_dir, 'method_local')
containing_dirname = os.path.basename(os.path.dirname(this_file_dir))

from pace_util import (
    pyhamilton, HamiltonInterface, LayoutManager,
    ResourceType, Plate24, Plate96, Tip96, LAYFILE,
    initialize, hepa_on, tip_pick_up, tip_eject, aspirate, dispense,
    resource_list_with_prefix, add_robot_level_log, add_stderr_logging,
    fileflag, clear_fileflag, log_banner)

if __name__ == '__main__':
    local_log_dir = os.path.join(method_local_dir, 'log')
    if not os.path.exists(local_log_dir):
        os.mkdir(local_log_dir)
    main_logfile = os.path.join(local_log_dir, 'main.log')
    logging.basicConfig(filename=main_logfile, level=logging.DEBUG, format='[%(asctime)s] %(name)s %(levelname)s %(message)s')
    add_robot_level_log()
    add_stderr_logging()
    for banner_line in log_banner('Begin execution of ' + __file__):
        logging.info(banner_line)

    debug = '--debug' in sys.argv
    prep_hard_agar = '--prep' in sys.argv
    simulation_on = debug or '--simulate' in sys.argv
    num_dilutions = 4
    num_plates = 1 if '--plates' not in sys.argv else int(sys.argv[sys.argv.index('--plates') + 1])
    num_skips = 0 if '--skip' not in sys.argv else int(sys.argv[sys.argv.index('--skip') + 1])
    culture_vol = 20 # uL
    hard_agar_vol = 220 # uL
    soft_agar_vol = 200 # uL
    culture_stock_vol = 300 # uL
    dipenses_per_prep_tip = int(1000/1.1)//int(hard_agar_vol)
    culture_asps_per_stock = int(culture_stock_vol)//int(culture_vol*1.25) # room for over-aspiration error
    #print('dipenses_per_prep_tip', dipenses_per_prep_tip)
    #print('culture_asps_per_stock', culture_asps_per_stock)

    lmgr = LayoutManager(LAYFILE)

    dilution_array = lmgr.assign_unused_resource(ResourceType(Plate96, 'dilutions'))
    agar_site = lmgr.assign_unused_resource(ResourceType(Plate96, 'agar'))
    culture_site = lmgr.assign_unused_resource(ResourceType(Plate96, 'culture'))
    plates = resource_list_with_prefix(lmgr, 'assay_plate_', Plate96, num_plates) # TODO: Plate24, num_plates)
    agar_tips = resource_list_with_prefix(lmgr, 'disposable_tips_', Tip96, 2)
    culture_tips = resource_list_with_prefix(lmgr, 'standard_tips_', Tip96, 2)

    agar_poss = ([(agar_site, 0), (agar_site, 1), (agar_site, 3), (agar_site, 4)]*((num_dilutions+1)//4))[:num_dilutions]
    skip_4th_chan = lambda n: n

    def agar_tips_gen():
        while True:
            for disp_tip_rack in agar_tips:
                for i in range(0, 96):
                    yield disp_tip_rack, i
    agar_tips_gen = agar_tips_gen()
    
    def culture_tips_gen():
        while True:
            for disp_tip_rack in culture_tips:
                for i in range(0, 96):
                    yield disp_tip_rack, i
    culture_tips_gen = culture_tips_gen()

    def agar_tip_batches():
        while True:
            yield skip_4th_chan([next(agar_tips_gen) for _ in range(num_dilutions)])
    agar_tip_batches = agar_tip_batches()

    def culture_tip_batches():
        while True:
            yield skip_4th_chan([next(culture_tips_gen) for _ in range(num_dilutions)])
    culture_tip_batches = culture_tip_batches()

    def culture_poss():
        while True:
            for start_i in range(0, 96, num_dilutions):
                for _ in range(culture_asps_per_stock):
                    yield [(culture_site, start_i + j) for j in range(num_dilutions)]
    culture_poss = culture_poss()

    def pos_batches(): # tuples (positions to aspirate from, positions to dispense into)
        for plate, quadrant_start in zip(plates, (0, 8*6, 4, 8*6+4)): # truncate to len(plates)
            for col in range(6): # column of 24-well plate
                yield ([(dilution_array, quadrant_start + col*8 + i) for i in range(num_dilutions)],
                       [(plate, idx_24_to_96(col*4 + i)) for i in range(num_dilutions)])
    pos_batches = pos_batches()
    
    def replace_agar_tips():
        n = 0
        while True:
            yield n % dipenses_per_prep_tip == 0
            n += 1
    replace_agar_tips = replace_agar_tips()
    
    dummy_24_plate = Plate24('')
    def idx_24_to_96(idx):
        col96, row96 = (c*2 for c in dummy_24_plate.well_coords(idx)) # upsample a 6x4 well plate to be accessed by a 12x8 template
        return col96*8 + row96

    with HamiltonInterface(simulate=simulation_on) as ham_int:
        ham_int.set_log_dir(os.path.join(local_log_dir, 'hamilton.log'))
        initialize(ham_int)
        #hepa_on(ham_int, simulate=int(simulation_on))
        try:
            errmsg_str = ''
            start_time = time.time()
            if debug or fileflag('debug'):
                clear_fileflag('debug') if not debug else ''; import pdb; pdb.set_trace()
            if fileflag('stop'):
                clear_fileflag('stop'); exit()

            def new_tips(ttype='agar'):
                while True:
                    try:
                        if ttype == 'agar':
                            tip_pick_up(ham_int, next(agar_tip_batches))
                        elif ttype == 'culture':
                            tip_pick_up(ham_int, next(culture_tip_batches))
                        else:
                            raise ValueError()
                        break
                    except pyhamilton.NoTipError:
                        initialize(ham_int) # eject tips if present
                        continue

            #########################
            # Begin non-boilerplate #
            #########################

            agar_class = 'HighVolumeFilter_Agar'
            std_class = 'StandardVolumeFilter_Water_DispenseJet_Empty_no_transport_vol'
            skips = num_skips
            for dilutions, plate_wells in pos_batches:
                if skips > 0:
                    logging.info('Skip ' + str(dilutions) + ', ' + str(plate_wells))
                    skips -= 1
                    continue
                if prep_hard_agar:
                    logging.info('\n##### Filling assay plates with hard agar.')
                    if next(replace_agar_tips):
                        tip_eject(ham_int)
                        new_tips()
                        aspirate(ham_int, agar_poss, [hard_agar_vol*dipenses_per_prep_tip*1.1]*num_dilutions, liquidClass=agar_class) # Aspirate 10% more to avoid bubbles
                    dispense(ham_int, plate_wells, [hard_agar_vol]*num_dilutions, liquidClass=agar_class)
                    continue
                logging.info('\n##### Moving culture into dilution wells.')
                new_tips('culture')
                aspirate(ham_int, next(culture_poss), [culture_vol]*num_dilutions, liquidClass=std_class)
                dispense(ham_int, dilutions, [culture_vol]*num_dilutions, liquidClass=std_class)
                tip_eject(ham_int)
                logging.info('\n##### Moving agar into dilution tubes.')
                new_tips()
                aspirate(ham_int, agar_poss, [soft_agar_vol + 50]*num_dilutions, liquidClass=agar_class)
                dispense(ham_int, skip_4th_chan(dilutions), skip_4th_chan([soft_agar_vol + 50]*num_dilutions), liquidHeight=6, liquidClass=agar_class)
                logging.info('\n##### Moving finished dilutions into plate wells.')
                aspirate(ham_int, skip_4th_chan(dilutions), skip_4th_chan([soft_agar_vol]*num_dilutions), liquidClass=agar_class)
                dispense(ham_int, skip_4th_chan(plate_wells), skip_4th_chan([soft_agar_vol]*num_dilutions), liquidHeight=6, liquidClass=agar_class)
                tip_eject(ham_int)
            if prep_hard_agar:
                tip_eject(ham_int)

        except Exception as e:
            errmsg_str = e.__class__.__name__ + ': ' + str(e).replace('\n', ' ')
            logging.exception(errmsg_str)
            print(errmsg_str)
        finally:
            clear_fileflag('debug')
            #if errmsg_str and not simulation_on and time.time() - start_time > 60*2:
            #    summon_devteam(__file__ + ' halted.' + (' There was an error: ' + errmsg_str + '; might want to look into that.' if errmsg_str else ''))

