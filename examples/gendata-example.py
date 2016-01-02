from mixer import Mixer, get_from_list, shuffled_ints, shuffled_decs

if __name__ == "__main__":
    fn = get_from_list("lists/firstnames.txt")
    ln = get_from_list("lists/lastnames.txt")
    dates_f = get_from_list("lists/dates.txt")
    systems_f = get_from_list("lists/systems.txt")
    device_agents_f = get_from_list("lists/device_agents.txt")

    mixer = Mixer()
    mixer.set_delimiter('|')
    mixer.add_column(fn)
    mixer.add_column(ln)
    mixer.add_column(shuffled_ints(1, 5))
    mixer.add_column(shuffled_ints(100, 250))
    mixer.add_column(dates_f)
    mixer.add_column(systems_f)
    mixer.add_column(device_agents_f)
    mixer.add_column(dates_f)
    mixer.add_column(['\n'])
    print mixer.mix(number_of_records=100, with_incremental_id=False)

    with open('sample-data.txt', "w+") as f:
        f.writelines(["first_name|last_name|count_num|amount|start_date|name_2|name_3|date_1|empty_col\n"])
        f.writelines(mixer.mix(number_of_records=100000, with_incremental_id=False))
