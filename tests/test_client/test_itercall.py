import mock
import unittest
import koji


class TestItercall(unittest.TestCase):

    def test_itercall(self):

        ks = koji.ClientSession('http://dumy.hub/address')
        ks.multiCall = mock.Mock(return_value=[['ret1'], ['ret2'], ['ret3']])

        args = ['arg1', 'arg2', 'arg3']
        rets = list(ks.itercall(args, lambda arg: ks.foo(arg)))

        ks.multiCall.assert_called_once_with()
        self.assertEquals(['ret1', 'ret2', 'ret3'], rets)


    def test_itercall_chunk_size(self):

        ks = koji.ClientSession('http://dumy.hub/address',
                                opts={'itercall_chunk_size': 2})
        mock_rets = [[[1], [2]], [[3], [4]], [[5], [6]], [[7]]]
        ks.multiCall = mock.Mock(side_effect=lambda: mock_rets.pop(0))

        args = [111, 222, 333, 444, 555, 666, 777]
        rets = list(ks.itercall(args, lambda arg: ks.foo(arg)))

        ks.multiCall.assert_has_calls([mock.call()] * 4)
        self.assertEquals(range(1,8), rets)
