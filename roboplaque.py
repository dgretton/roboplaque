#!python3

import os
import sys
import time
import logging
from math import ceil

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
    assert num_plates <= 4
    num_skips = 0 if '--skip' not in sys.argv else int(sys.argv[sys.argv.index('--skip') + 1])
    culture_vol = 20 # uL
    hard_agar_vol = 250 # uL
    soft_agar_vol = 200 # uL
    culture_stock_vol = 300 # uL
    dipenses_per_prep_tip = 3 #int(1000/1.1)//int(hard_agar_vol)
    culture_asps_per_stock = int(culture_stock_vol)//int(culture_vol*1.25) # room for over-aspiration error
    #print('dipenses_per_prep_tip', dipenses_per_prep_tip)
    #print('culture_asps_per_stock', culture_asps_per_stock)
    #print('Number of filled', culture_stock_vol, 'uL tubes of bacterial culture needed for this run:', int(ceil(num_plates*24/culture_asps_per_stock)))

    lmgr = LayoutManager(LAYFILE)

    dilution_array = lmgr.assign_unused_resource(ResourceType(Plate96, 'dilutions'))
    agar_sites = resource_list_with_prefix(lmgr, 'agar_', Plate96, 4)
    culture_site = lmgr.assign_unused_resource(ResourceType(Plate96, 'culture'))
    plates = resource_list_with_prefix(lmgr, 'assay_plate_', Plate24, num_plates)
    agar_tips = resource_list_with_prefix(lmgr, 'disposable_tips_', Tip96, 1)
    culture_tips = resource_list_with_prefix(lmgr, 'standard_tips_', Tip96, 1)
    #eject_site = None if '--test' in sys.argv else lmgr.assign_unused_resource(ResourceType(Tip96, 'test_eject_site'))

    all_agar_poss = [(agar_site, n) for agar_site in agar_sites for n in (0, 1)]

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

    def gen_culture_poss():
        while True:
            for _ in range(culture_asps_per_stock):
                for start_i in range(0, 16, num_dilutions):
                    yield [(culture_site, start_i + j) for j in range(num_dilutions)]
    gen_culture_poss = gen_culture_poss()

    def pos_batches(): # tuples (positions to aspirate from, positions to dispense into)
        for plate, quadrant_start in zip(plates, (0, 4, 8*6, 8*6+4)): # truncate to len(plates)
            for col in range(6): # column of 24-well plate
                yield ([(dilution_array, quadrant_start + col*8 + i) for i in range(num_dilutions)],
                       [(plate, col*4 + i) for i in range(num_dilutions)],
                       lambda: next(gen_culture_poss))
    pos_batches = list(pos_batches())
    
    def replace_agar_tips():
        n = 0
        while True:
            yield n % dipenses_per_prep_tip == 0
            n += 1
    replace_agar_tips = replace_agar_tips()

    with HamiltonInterface(simulate=simulation_on) as ham_int:
        ham_int.set_log_dir(os.path.join(local_log_dir, 'hamilton.log'))
        initialize(ham_int)
        hepa_on(ham_int, 30, simulate=int(simulation_on))
        exit()
        try:
            errmsg_str = ''
            start_time = time.time()
            if debug or fileflag('debug'):
                clear_fileflag('debug') if not debug else ''; import pdb; pdb.set_trace()
            if fileflag('stop'):
                clear_fileflag('stop'); exit()

            def new_tips(num=num_dilutions, ttype='agar'):
                while True:
                    try:
                        if ttype == 'agar':
                            tip_batch = [next(agar_tips_gen) for _ in range(num)]
                        elif ttype == 'culture':
                            tip_batch = [next(culture_tips_gen) for _ in range(num)]
                        else:
                            raise ValueError()
                        tip_pick_up(ham_int, tip_batch)
                        break
                    except pyhamilton.NoTipError:
                        initialize(ham_int) # eject tips if present
                        continue
                return tip_batch

            #########################
            # Begin non-boilerplate #
            #########################

            agar_class = 'HighVolumeFilter_Agar'
            std_class = 'StandardVolumeFilter_Water_DispenseJet_Empty_no_transport_vol'

            def tend_to(dilutions, plate_wells, culture_poss): # deal with up to 8 at the same time
                print(culture_poss)
                num_dilutions = len(dilutions)
                print(num_dilutions)
                agar_poss = all_agar_poss[:num_dilutions]
                if prep_hard_agar:
                    logging.info('\n##### Filling assay plates with hard agar.')
                    if next(replace_agar_tips):
                        tip_eject(ham_int)
                        new_tips(num_dilutions)
                        aspirate(ham_int, agar_poss, [hard_agar_vol*dipenses_per_prep_tip*1.3]*num_dilutions, liquidClass=agar_class) # Aspirate 10% more to avoid bubbles
                    dispense(ham_int, plate_wells, [hard_agar_vol]*num_dilutions, liquidHeight=6, liquidClass=agar_class)
                    return
                logging.info('\n##### Moving culture into dilution wells.')
                new_tips(num_dilutions, 'culture')
                aspirate(ham_int, culture_poss, [culture_vol]*num_dilutions, liquidClass=std_class)
                dispense(ham_int, dilutions, [culture_vol]*num_dilutions, liquidClass=std_class)
                tip_eject(ham_int)
                logging.info('\n##### Moving agar into dilution tubes.')
                new_tips(num_dilutions)
                aspirate(ham_int, agar_poss, [soft_agar_vol + 50]*num_dilutions, liquidClass=agar_class)
                dispense(ham_int, dilutions, [soft_agar_vol + 50]*num_dilutions, liquidHeight=6, liquidClass=agar_class)
                logging.info('\n##### Moving finished dilutions into plate wells.')
                aspirate(ham_int, dilutions, [soft_agar_vol]*num_dilutions, liquidClass=agar_class)
                dispense(ham_int, plate_wells, [soft_agar_vol]*num_dilutions, liquidHeight=6, liquidClass=agar_class)
                tip_eject(ham_int)

            todo_plates = plates[:]
            while todo_plates:
                this_round_plates = []
                for _ in range(2):
                    try:
                        this_round_plates.append(todo_plates.pop(0))
                    except IndexError:
                        break
                this_round_batches = []
                for pos_batch in pos_batches:
                    _, ((extract_plate, _), *_), _ = pos_batch
                    if extract_plate in this_round_plates:
                        this_round_batches.append(pos_batch)
                if len(this_round_batches) > 6: # divide it in half
                    if len(this_round_batches)%2 != 0:
                        raise RuntimeError()
                    half_idx = len(this_round_batches)//2
                    for (dilutions1, plate_wells1, culture_poss1), (dilutions2, plate_wells2, culture_poss2) in zip(this_round_batches[:half_idx], this_round_batches[half_idx:]):
                        dilutions = dilutions1 + dilutions2
                        plate_wells = plate_wells1 + plate_wells2
                        culture_poss = culture_poss1() + culture_poss2()
                        tend_to(dilutions, plate_wells, culture_poss)
                else:
                    for dilutions, plate_wells, culture_poss in this_round_batches:
                        tend_to(dilutions, plate_wells, culture_poss())
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

