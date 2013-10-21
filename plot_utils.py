# -*- coding: utf-8 -*-
"""
  plotting utilities


"""
import numpy as np
import matplotlib.pyplot as plt

###  plots 
def show_plot(plt):                      
    print 'Paused for plot review. Close plot window to continue.'
    plt.show()
    plt.close()
    

def save_plot(plt, fname):
    plt.draw()
    plt.savefig(fname, transparent=False ) #, bbox_inches="tight")
    plt.close()


def aplot(array_dict={}, title='array plot', grid=True, legend=True):
    '''plot a dictionary of up to four arrays, with legend by default
        example dict:  {'Airspeed': airspeed.array }
    '''
    print 'title:', title
    if len(array_dict.keys())==0:
        print 'Nothing to plot!'
        return
    figure = plt.figure()
    figure.set_size_inches(10,5)
    series_names = array_dict.keys()[:4]  #only first 4
    series_formats = ['k','g','b','r']    #color codes
    for i,nm in enumerate(series_names):
        plt.plot(array_dict[nm], series_formats[i])
    if grid: plt.grid(True, color='gray')
    plt.title(title)
    if legend: 
        leg = plt.legend(series_names, 'upper left', fancybox=True)
        leg.get_frame().set_alpha(0.5)        
    plt.xlabel('sample index')
    print 'Paused for plot review. Close plot window to continue.'
    plt.show()
    plt.clf()
    plt.close()


def lplot(myhdf5, params=('Airspeed','Altitude STD')):
    '''plot a list of parameters from an hdf5 file'''
    pdict={}
    for p in params:
        pdict[p]=myhdf5.get(p).array
    aplot(pdict)


if __name__=='__main__':
    myplt = aplot({'first': np.array([4,8,2,4,6,9])})
    #show_plot(myplt)