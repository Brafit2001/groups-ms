

class Rubric:

    def __init__(self, rubricId, name):
        self.id = rubricId
        self.name = name

    def to_json(self):
        return {
            "id": self.id,
            "name": self.name
        }


def row_to_rubric(row):
    return Rubric(
        rubricId=row[0],
        name=row[1]
    )
