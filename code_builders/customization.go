package builders

var (
	registeredGenerators = map[string]CallFunctionDescriber{
		"now": TimeNowFn,
	}
)

func AddNewGenerator(name string, descr CallFunctionDescriber) {
	registeredGenerators[name] = descr
}
