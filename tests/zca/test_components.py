def test_component_registry():
    from zope import component, interface

    gsm = component.getSiteManager()

    class IFoo(interface.Interface):
        pass

    class IBar(interface.Interface):
        pass

    @interface.implementer(IBar)
    @component.adapter(IFoo)
    class Bar:
        def __init__(self, foo):
            self.foo = foo

    @interface.implementer(IFoo)
    class Foo:
        pass

    gsm.registerAdapter(Bar)

    foo = Foo()
    bar = IBar(foo)

    assert bar.foo is foo
