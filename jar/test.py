import unittest
import jar
import torch
import numpy as np


class TestJar(unittest.TestCase):

    def test_sanity(self):
        self.assertTrue(1 + 1 == 2)
        self.assertFalse(1 + 1 == 3)

    def test_dump_str(self):
        s = 'hello'
        jar.dump(s, 'test_dump_str')
        s1 = jar.load('test_dump_str')
        self.assertEqual(s, s1)

    def test_dump_dict(self):
        d = {
            'a': 'a',
            'b': 'b'
        }
        jar.dump(d, 'test_dump_dict')
        d1 = jar.load('test_dump_dict')
        self.assertEqual(d, d1)

    def test_dump_np(self):
        a = np.random.rand(100, 100, 100)
        jar.dump(a, 'test_dump_np')
        a1 = jar.load('test_dump_np')
        self.assertTrue((a == a1).all())

    def test_dump_state_dict(self):
        class Net(torch.nn.Module):
            def __init__(self):
                super(Net, self).__init__()
                self.fc1 = torch.nn.Linear(2000, 2000)
                self.fc2 = torch.nn.Linear(200, 200)
                self.conv = torch.nn.Conv2d(64, 128, 3)
                self.dropout = torch.nn.Dropout(p=.1)

        net = Net()
        d = net.state_dict()

        jar.dump(d, 'test_dump_state_dict')
        d1 = jar.load('test_dump_state_dict')

        self.assertEqual(d.keys(), d1.keys())
        for key in d.keys():
            self.assertTrue((
                d[key] == d1[key]
            ).all())


if __name__ == "__main__":
    unittest.main()
