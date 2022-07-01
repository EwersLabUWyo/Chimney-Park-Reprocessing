from pathlib import Path

import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mpl_dates

class analyze:
    """sub-class of the fast processing engine designed to aid with post-processing analysis"""
    
    def __init__(self, processor):
        """processor: an instance of the fast_processing_engine class that has already been run"""
        self.summary = processor.summary
        self.site_info = processor.site_info
        
        self.colors = {}
        
        self.view = {}
        self.axes = {}
        self.flags = {}
        self.originals = {}
        
    def make_timeseries_plot(self, name, sites, column, w=10, h=5):
        """create a new plot variable for the given sites and columns"""
        sub_summary = self.summary[column].sel(SITE=sites)
        
        fig, axs = plt.subplots(2, 1, sharex='col', figsize=(w, h))
        fig.suptitle(column)
        
        for i, site in enumerate(sites):
            self.colors[site] = f'C{i}'
            
            dates = sub_summary.TIMESTAMP.data
            x = mpl_dates.date2num(dates)
            
            axs[0].set_title('Deviations from average')
            ymax = sub_summary.sel(SITE=site, STAT='Max')
            ymin = sub_summary.sel(SITE=site, STAT='Min')
            ystd = sub_summary.sel(SITE=site, STAT='Std')
            yavg = sub_summary.sel(SITE=site, STAT='Avg')
            axs[0].plot_date(x, ymax, linestyle='--', fmt='none', color=self.colors[site], lw=0.5)
            axs[0].plot(x, ymin, linestyle='--', color=self.colors[site], lw=0.5)
            axs[0].fill_between(x, yavg - ystd, yavg + ystd, alpha=0.3, color=self.colors[site])
            axs[0].plot(x, yavg, color=self.colors[site], label=site)
            
            axs[1].set_title('% Invalid measurements')
            ynpc = sub_summary.sel(SITE=site, STAT='Npc')
            axs[1].plot(x, ynpc, color=self.colors[site], label=site)
            axs[1].plot(x, 0*x, color='k', linewidth='0.2', linestyle='--')
            axs[1].set_ylim(-5, 105)
            plt.legend()
            
        self.axes[name] = axs
        self.view[name] = fig
        
        self.originals[name] = (name, sites, column, w, h, 'timeseries')
    
    def zoom_timeseries(self, name, ylims=None, xlims=None, xspan=None):
        """zooms in on a timeseries plot. 
        if ylims is provided, it should be a tuple of (ymin, ymax)
        if xlims is provided, the plot zooms to the provided limits. Follows default formatting used by pandas.to_datetime, so 'yyyy-mm-dd hh:mm', where hh and mm are optional.
        if xlims is NOT provided, then you can provide instead as a tuple of (start, span). Format start the same as you would for xlims (so yyyy-mm-dd hh:mm). Format the span as <period><units> in the pandas.Timedelta format"""
        
        if ylims is not None:
            self.axes[name][0].set_ylim(*ylims)
        if xlims is not None:
            
            self.axes[name][0].set_xlim(mpl_dates.date2num(pd.to_datetime(xlims[0])), 
                                        mpl_dates.date2num(pd.to_datetime(xlims[1])))
            self.axes[name][1].set_xlim(mpl_dates.date2num(pd.to_datetime(xlims[0])), 
                                        mpl_dates.date2num(pd.to_datetime(xlims[1])))
        elif xspan is not None:
            xmin = pd.to_datetime(xspan[0])
            xmax = xmin + pd.Timedelta(xspan[1])
            xmin, xmax = mpl_dates.date2num(xmin), mpl_dates.date2num(xmax)
            self.axes[name][0].set_xlim(xmin, xmax)
            self.axes[name][1].set_xlim(xmin, xmax)
            
        
        display(self.view[name])
        
    
    def pan(self, name, amount):
        """nudges the x axis of a plot inwards or outwards. Uses pandas.Timedelta format for 'amount'"""
        
        xmin, xmax = self.axes[name][0].get_xlim()
        xmin, xmax = mpl_dates.num2date(xmin), mpl_dates.num2date(xmax)
        xmin = xmin + pd.Timedelta(amount)
        xmax = xmax + pd.Timedelta(amount)
        xmin, xmax = mpl_dates.date2num(xmin), mpl_dates.date2num(xmax)
        
        self.axes[name][0].set_xlim(xmin, xmax)
        self.axes[name][1].set_xlim(xmin, xmax)
        
        display(self.view[name])
        
    def flag(self, name, ID, sites):
        """flags the current limits as 'bad'
        name: your current plot
        ID: something to call this flag instance
        sites: sites to apply flag to
        """
        xmin, xmax = self.axes[name][0].get_xlim()
        xmin, xmax = mpl_dates.num2date(xmin), mpl_dates.num2date(xmax)
        self.flags[ID] = (xmin, xmax, sites)
        
    def plot_flags(self, name):
        self.revert_view(name)
        for ID in self.flags:
            for site in self.flags[ID][-1]:
                flag_xmin = mpl_dates.date2num(self.flags[ID][0])
                flag_xmax = mpl_dates.date2num(self.flags[ID][1])
                self.axes[name][0].fill_betweenx([*self.axes[name][0].get_ylim()], 
                                                 flag_xmin, flag_xmax, 
                                                 color=self.colors[site], alpha=0.5, label=ID)
                self.axes[name][1].fill_betweenx([-5, 105], 
                                                 flag_xmin, flag_xmax, 
                                                 color=self.colors[site], alpha=0.5, label=ID)
        
        plt.legend()        
        display(self.view[name])
                
        
    def revert_view(self, name):
        """revert to view limits to the original"""
        if self.originals[name][-1] == 'timeseries':
            self.make_timeseries_plot(*self.originals[name][:-1])
            
    def revert_flags(self, sites, IDs):
        """provide a sequence of IDs of flags to remove, or simply put 'all' to remove all flags."""
        if IDs == 'all':
            self.flags = {}
        else:
            for ID in IDs:
                self.flags.pop(ID)
                
        
        
    