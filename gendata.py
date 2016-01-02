from mixer import Mixer, get_from_list, shuffled_ints

if __name__ == "__main__":
    fn = get_from_list("lists/firstnames.txt", 2)
    ln = get_from_list("lists/lastnames.txt", 3)

    mixer = Mixer()

    mixer.set_delimiter('|')

    mixer.add_column(fn)
    mixer.add_column(ln)

    mixer.add_column(shuffled_ints(1, 5))

    print mixer.mix()


    mixer.add_record("")

