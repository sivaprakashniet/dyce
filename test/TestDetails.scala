object TestDetails {
  val local_filepath = "test/res/sample_dataset.csv"
  val s3path = "/sample_dataset.csv"
  val user_id = "1"
  val incorrect_user_id = "2"

  val dataset_schema_before_schema_creation =
    """{"file_path":"/sample_dataset.csv","file_size":1000,
      "dataset_name":""""+s3path+"""", "parquet_file_path":""}"""

  val dataset_schema_after_schema_creation =
    """{"file_path":"/sample_dataset_new.csv","file_size":1000,
      "dataset_name":""""+s3path+"""", "parquet_file_path":""}"""

  val dataset_schema_rename =
    """{"file_path":"/sample_dataset_renamed.csv",
      "dataset_name":"sample_dataset_renamed"}"""

  val create_d_tree_json =
    """{
      "name": "Testing decision tree",
      "input_column": ["segment","MOB"],
      "output_column": "SegmentAccts",
      "dataset_id": "7a0a537b-7252-4073-89a7-a36b3c5e8546",
      "dataset_name": "demo_with_new_add_cl",
      "parameters":
      {
      "impurity": "entropy",
      "max_bins": 32,
      "max_depth": 5,
      "min_instances_per_node": 1,
      "training_data": 80,
      "test_data": 20
      }
      }""".stripMargin

  val column_schema_before_schema_creation = """{"schema_list": [{
                                "name": "Segment","description":"","position": 1,"datatype": "String",
                                "format": "string","display_format": "","separator": false,
                                "decimal": 2,"visibility": false,
                                "calculated": true,"formula": "","metrics": ""}]}"""

  val column_schema_after_schema_creation = """{"schema_list": [{
                                "id":"8b91ef28-50d0-4ec1-9c9e-8ec73df7fb10",
                                "name": "Segment","description":"","position": 1,"datatype": "String",
                                "format": "string","display_format": "","separator": false,
                                "decimal": 2,"visibility": false,
                                "calculated": true,"formula": "","metrics": "",
                                "created_date":"12345"}]}"""

  val update_dataset_json = """{
                                "schema_list": [
                                  {
                                    "id": "1054e25c-68c3-40de-a8ac-1c91b5892d81",
                                    "name": "Id",
                                    "new_name": "Id",
                                    "position": 1,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "d0090914-f03e-4c42-b007-736a52f60781",
                                    "name": "Name",
                                    "new_name": "Name",
                                    "position": 2,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "e0b4ec4a-f790-4eca-a3a9-a2f6f8ed853c",
                                    "name": "Address",
                                    "new_name": "Address",
                                    "position": 3,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "6871ee19-64aa-46b6-802d-bd0d0d1bdf19",
                                    "name": "Marks_in_maths",
                                    "new_name": "Marks_in_maths",
                                    "position": 4,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "df841b21-07de-4a4e-8a49-1f1d4359f63e",
                                    "name": "Marks_in_science",
                                    "new_name": "Marks_in_science",
                                    "position": 5,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "3c5dd98e-d7da-4b7c-b2da-c4046621194e",
                                    "name": "Percentage_in_maths",
                                    "new_name": "Percentage_in_maths",
                                    "position": 6,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "28bdfc3d-f285-4b48-8ea1-69e62ae40728",
                                    "name": "Percentage_in_science",
                                    "new_name": "Percentage_in_science",
                                    "position": 7,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "3e02c9a8-bf9d-46bb-8014-ce8809460d66",
                                    "name": "Total_percentage",
                                    "new_name": "Total_percentage",
                                    "position": 8,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "7636028f-26c6-4be9-b1e8-8185b92c0ca3",
                                    "name": "Grade",
                                    "new_name": "Grade",
                                    "position": 9,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "b94d11e9-6bc7-440b-a46e-5daeae0c5ab0",
                                    "name": "Attended_all_classes",
                                    "new_name": "Attended_all_classes",
                                    "position": 10,
                                    "description":"",
                                    "datatype": "Category",
                                    "format": "Category",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": false,
                                    "formula": "",
                                    "metrics": "",
                                    "status": "unmodified"
                                  },
                                  {
                                    "id": "",
                                    "name": "new_col",
                                    "new_name": "new_col",
                                    "position": 11,
                                    "description":"",
                                    "datatype": "AutoDetect",
                                    "format": "",
                                    "display_format": "",
                                    "separator": false,
                                    "decimal": 0,
                                    "visibility": false,
                                    "calculated": true,
                                    "formula": "CONCAT([Name],[Id])",
                                    "metrics": "",
                                    "status": "new"
                                  }
                                ]
                              }"""

  val inferschema_test_json = """{


	"schema_list": [
		{
			"name": "Id",
			"position": 1,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Name",
			"position": 2,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Address",
			"position": 3,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Marks in maths",
			"position": 4,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Marks in science",
			"position": 5,
			"datatype": "String",
      "format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Percentage in maths",
			"position": 6,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Percentage in science",
			"position": 7,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Total percentage",
			"position": 8,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Grade",
			"position": 9,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		},
		{
			"name": "Attended all classes",
			"position": 10,
			"datatype": "String",
			"format": "String",
      "display_format": "",
			"decimal": 0,
			"separator": false,
			"visibility": false,
			"calculated": false,
			"formula": "",
			"metrics": ""
		}
	],
	"preview_data": [
		[
			"1",
			"A",
			"121 - Street",
			"1500",
			"2500.46",
			"0.5",
			"0.8334866667",
			"66.6743333333",
			"D",
			"0"
		],
		[
			"2",
			"B",
			"122 - Street",
			"2000",
			"2440.23",
			"0.6666666667",
			"0.81341",
			"74.0038333333",
			"C",
			"0"
		],
		[
			"3",
			"B",
			"123 - Street",
			"3000",
			"2986.112",
			"1",
			"0.9953706667",
			"99.7685333333",
			"A",
			"1"
		],
		[
			"4",
			"C",
			"124 - Street",
			"500",
			"329.333",
			"0.1666666667",
			"0.1097776667",
			"13.8222166667",
			"F",
			"0"
		],
		[
			"5",
			"B",
			"125 - Street",
			"2001",
			"3000",
			"0.667",
			"1",
			"83.35",
			"B",
			"1"
		],
		[
			"6",
			"C",
			"126 - Street",
			"1002",
			"1500",
			"0.334",
			"0.5",
			"41.7",
			"F",
			"0"
		],
		[
			"7",
			"A",
			"127 - Street",
			"524",
			"3000",
			"0.1746666667",
			"1",
			"58.7333333333",
			"E",
			"0"
		],
		[
			"8",
			"C",
			"128 - Street",
			"2230",
			"2342.32",
			"0.7433333333",
			"0.7807733333",
			"76.2053333333",
			"C",
			"0"
		],
		[
			"9",
			"B",
			"129 - Street",
			"3000",
			"3000",
			"1",
			"1",
			"100",
			"A",
			"0"
		],
		[
			"10",
			"A",
			"130 - Street",
			"403",
			"356.23",
			"0.1343333333",
			"0.1187433333",
			"12.6538333333",
			"F",
			"0"
		],
		[
			""
		]
	]
}"""
}
