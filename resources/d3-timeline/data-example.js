var processes = [
	"PaI",
	"PcI",
	"PbI",
];

var workflowData = [
	{id: "1", data: [
		{label: "PaI", times: [
			{"label":"0", "process": "PaI", "starting_time": 1540595694411, "ending_time": 1540595694415, "result":"PiPair(PiItem(1),PiItem(1))"},
		]},
		{label: "PcI", times: [
			{"label":"1", "process": "PcI", "starting_time": 1540595694417, "ending_time": 1540595694518, "result":"PiItem(PcISleptFor1s)"},
		]},
		{label: "PbI", times: [
			{"label":"2", "process": "PbI", "starting_time": 1540595694417, "ending_time": 1540595694519, "result":"PiItem(PbISleptFor1s)"},
		]},
	]},

	{id: "0", data: [
		{label: "PaI", times: [
			{"label":"0", "process": "PaI", "starting_time": 1540595694410, "ending_time": 1540595694412, "result":"PiPair(PiItem(1),PiItem(1))"},
		]},
		{label: "PcI", times: [
			{"label":"1", "process": "PcI", "starting_time": 1540595694414, "ending_time": 1540595694515, "result":"PiItem(PcISleptFor1s)"},
		]},
		{label: "PbI", times: [
			{"label":"2", "process": "PbI", "starting_time": 1540595694415, "ending_time": 1540595694516, "result":"PiItem(PbISleptFor1s)"},
		]},
	]},

	{id: "2", data: [
		{label: "PaI", times: [
			{"label":"0", "process": "PaI", "starting_time": 1540595694412, "ending_time": 1540595694418, "result":"PiPair(PiItem(1),PiItem(1))"},
		]},
		{label: "PcI", times: [
			{"label":"1", "process": "PcI", "starting_time": 1540595694420, "ending_time": 1540595694520, "result":"PiItem(PcISleptFor1s)"},
		]},
		{label: "PbI", times: [
			{"label":"2", "process": "PbI", "starting_time": 1540595694420, "ending_time": 1540595694521, "result":"PiItem(PbISleptFor1s)"},
		]},
	]},

];