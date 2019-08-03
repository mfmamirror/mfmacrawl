import re


def path_identifier(path):
    identifier = path
    identifier.replace(" ", "_")
    # - identifier must be alphanum, - or _
    # - identifier must be 5-100 chars long
    # - identifier must be lower case
    identifier = re.sub("[^\w-]+", "_", identifier).lower()[-100:]
    # identifier must start with alphanum
    identifier = re.sub("^[^a-z0-9]+", "", identifier)
    return identifier


# get or create bucket
# get or create key
# check file for updates
# if updated, upload the new version
