class QueryParameters:

    def __init__(self, request):
        self.id = request.args.get("ids")
        self.description = request.args.get("description")
        self.class_attribute = request.args.get("class")
        self.name = request.args.get("name")
        self.group = request.args.get("group")
        self.subject = request.args.get("subject")
        self.course = request.args.get("course")
        self.year = request.args.get("year")
        self.title = request.args.get("title")
        self.deadline = request.args.get("deadline")
        self.unit = request.args.get("unit")

    def add_to_query(self, query: str):
        for param in self.__dict__:
            param_value = getattr(self, param)
            if param_value is not None:
                if param == "class_attribute":
                    column = "class"
                else:
                    column = param
                if "where" not in query:
                    query += f" where `{column}` in {self.parseParam(param, param_value)}"
                else:
                    query += f" and `{column}` in {self.parseParam(param, param_value)}"
        return query

    def parseParam(self, param, value):
        if param == "id":
            id_list = self.id.split(',')
            if len(id_list) == 1:
                return f"('{id_list[0]}')"
            else:
                return tuple(self.id.split(','))
        else:
            return f"('{value}')"
