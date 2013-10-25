# -*- coding: utf-8 -*-
"""
test_apt_rwy.py
@author: KEITHC

from ipython, 
 cd to this directory
 run '!nosetests -v test_apt_rwy.py'
"""
import unittest
import load_apt_rwy as ld

# opposite_runway()
class TestOpposite(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_opp_22R(self):
        print '22R', ld.opposite_runway('22R')
        self.assertEqual(ld.opposite_runway('22R'), '04L')
    def test_opp_04L(self):
        self.assertEqual(ld.opposite_runway('04L'), '22R')
    
    def test_opp_36(self):
        self.assertEqual(ld.opposite_runway('36'), '18')
    
    def test_opp_01(self):
        self.assertEqual(ld.opposite_runway('01'), '19')
    
    def test_opp_18(self):
        self.assertEqual(ld.opposite_runway('18'), '36')

    

# jeplat/lon2dd()
class TestDD(unittest.TestCase):
    def setUp(self):
        pass       
    def tearDown(self):
        pass
    def test_lat_1(self):
        mylat='N40-41-32.99'
        print ld.jeplat2dd(mylat)
        self.assertAlmostEqual(ld.jeplat2dd(mylat), 40.6947222222)   
    def test_lat_2(self):
        mylat='N40-00-00.00'
        print ld.jeplat2dd(mylat)
        self.assertAlmostEqual(ld.jeplat2dd(mylat), 40.0 )    
    def test_lat_3(self):
        mylat='N00-00-00.00'
        print ld.jeplat2dd(mylat)
        self.assertAlmostEqual(ld.jeplat2dd(mylat), 0.0 )    
    def test_lon_1(self):
        mylon='W074-10-07.18'
        self.assertAlmostEqual(ld.jeplon2dd(mylon),-73.8305555556)    
    def test_lon_2(self):
        mylon='W000-00-00.00'
        self.assertAlmostEqual(ld.jeplon2dd(mylon),0.0)


if __name__ == '__main__':
    unittest.main(exit=False, verbosity=2)
    
