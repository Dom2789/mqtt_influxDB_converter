
def topic_matches(pattern: str, topic: str) -> bool:
    p_level = pattern.split("/")
    t_level = topic.split("/")

    for i, p in enumerate(p_level):
        # multi-level wildcards
        if p == "#":
            if i == 0 and t_level and t_level[0][0] == "$":
                return False
            return True

        # catches pattern longer as filter
        if i >= len(t_level):
            return False

        # single-level wildcard
        if p == "+":
            if i == 0 and t_level and t_level[0][0] == "$":
                return False
            continue

        if p != t_level[i]:
            return False

        

    # catches topic longer as pattern
    return len(p_level) == len(t_level)


def tags_from_topic():
    pass